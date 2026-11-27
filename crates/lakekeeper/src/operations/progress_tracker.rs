use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio_util::sync::CancellationToken;

/// Information about a sub-tracker
#[derive(Debug)]
struct SubTrackerInfo {
    // Strong reference to the sub-tracker
    tracker: Arc<ProgressTrackerInner>,
    // Weight of this sub-tracker in the parent (0.0 to 1.0)
    weight: f64,
}

/// Inner data of the progress tracker
#[derive(Debug)]
struct ProgressTrackerInner {
    // Current progress (0 to total)
    current: AtomicU64,
    // Total progress value (100% corresponds to this value)
    total: AtomicU64,
    // Description of this tracker (immutable)
    description: String,
    // Cancellation token to signal that the operation should be cancelled
    cancellation_token: CancellationToken,
    // Sub-trackers that contribute to this tracker's progress
    sub_trackers: Mutex<Vec<SubTrackerInfo>>,
}

/// A progress tracker that can be used to track progress of operations.
/// It uses atomic integers for thread-safe updates and supports cancellation via `CancellationToken`.
///
/// The tracker supports creating sub-trackers that will contribute to the parent's progress,
/// allowing for composition of operations with different progress weights.
#[derive(Debug, Clone)]
pub struct ProgressTracker {
    // We use Arc to share the inner data between clones
    inner: Arc<ProgressTrackerInner>,
}

impl ProgressTracker {
    /// Helper method to safely lock a mutex and recover from poisoning
    ///
    /// Mutex poisoning occurs when a thread panics while holding the mutex lock.
    /// However, in our case, we can safely recover and use the "poisoned" data because:
    ///
    /// 1. **Data integrity is preserved**: The mutex data structure itself (Vec<SubTrackerInfo>)
    ///    remains valid and accessible even after a panic. Poisoning only indicates that
    ///    a panic occurred while the mutex was locked, not that the data is corrupted.
    ///
    /// 2. **Atomic operations**: Our progress tracking relies on atomic operations for
    ///    critical data (current/total progress values), so the core progress state
    ///    remains consistent regardless of mutex poisoning.
    ///
    /// 3. **Graceful degradation**: In the worst case, we might have slightly stale
    ///    sub-tracker information, but this doesn't compromise the overall functionality
    ///    or safety of progress tracking - it's better to continue with potentially
    ///    outdated progress info than to crash the entire operation.
    ///
    /// 4. **Non-critical data**: The mutex protects sub-tracker metadata which is used
    ///    for progress aggregation and display purposes, not for critical application logic.
    fn lock_mutex<'a, T>(mutex: &'a Mutex<T>, context: &str) -> std::sync::MutexGuard<'a, T> {
        mutex.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("{context} mutex was poisoned, recovering data");
            poisoned.into_inner()
        })
    }

    /// Safe conversion from u64 to f64, allowing precision loss
    #[allow(clippy::cast_precision_loss)]
    fn u64_to_f64(value: u64) -> f64 {
        value as f64
    }

    /// Calculate this tracker's own progress from its current and total values
    /// Returns a float between 0.0 and 1.0
    fn own_progress(&self) -> f64 {
        Self::calculate_progress_from_inner(&self.inner)
    }

    /// Calculate progress from a [`ProgressTrackerInner`]'s current and total values
    /// Returns a float between 0.0 and 1.0
    fn calculate_progress_from_inner(inner: &ProgressTrackerInner) -> f64 {
        let current = Self::u64_to_f64(inner.current.load(Ordering::Relaxed));
        let total = Self::u64_to_f64(inner.total.load(Ordering::Relaxed));
        if total > 0.0 { current / total } else { 0.0 }
    }

    /// Create a new progress tracker with default settings:
    /// - Initial total: 100
    /// - New cancellation token
    /// - Default description: "Tracker"
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ProgressTrackerInner {
                current: AtomicU64::new(0),
                total: AtomicU64::new(100),
                description: "Tracker".to_string(),
                cancellation_token: CancellationToken::new(),
                sub_trackers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Create a new progress tracker with a specified cancellation token
    /// - Default description: "Tracker"
    #[must_use]
    pub fn new_with_cancellation_token(token: CancellationToken) -> Self {
        Self {
            inner: Arc::new(ProgressTrackerInner {
                current: AtomicU64::new(0),
                total: AtomicU64::new(100),
                description: "Tracker".to_string(),
                cancellation_token: token,
                sub_trackers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Create a new progress tracker with a custom description
    #[must_use]
    pub fn new_with_description(description: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ProgressTrackerInner {
                current: AtomicU64::new(0),
                total: AtomicU64::new(100),
                description: description.into(),
                cancellation_token: CancellationToken::new(),
                sub_trackers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Create a new progress tracker with a custom description and cancellation token
    #[must_use]
    pub fn new_with_description_and_token(
        description: impl Into<String>,
        token: CancellationToken,
    ) -> Self {
        Self {
            inner: Arc::new(ProgressTrackerInner {
                current: AtomicU64::new(0),
                total: AtomicU64::new(100),
                description: description.into(),
                cancellation_token: token,
                sub_trackers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Set the total progress value
    pub fn set_total(&self, total: u64) {
        self.inner.total.store(total, Ordering::Release);
    }

    /// Set total and reset progress to 0
    pub fn set_total_and_reset(&self, total: u64) {
        self.inner.total.store(total, Ordering::Release);
        self.inner.current.store(0, Ordering::Release);
    }

    /// Get the total progress value
    #[must_use]
    pub fn total(&self) -> u64 {
        self.inner.total.load(Ordering::Relaxed)
    }

    /// Get the progress as a float between 0.0 and 1.0
    #[must_use]
    pub fn progress(&self) -> f64 {
        let sub_trackers = Self::lock_mutex(&self.inner.sub_trackers, "Sub-trackers");

        // If there are no sub-trackers, return the normalized progress from stored values
        if sub_trackers.is_empty() {
            return self.own_progress();
        }

        // Calculate the total weighted progress from all sub-trackers
        let mut total_weighted_progress = 0.0;

        for sub_info in sub_trackers.iter() {
            // Create a temporary wrapper to access sub-tracker methods
            let sub_tracker = ProgressTracker {
                inner: Arc::clone(&sub_info.tracker),
            };

            // Get normalized progress from the sub-tracker (which will recursively calculate if needed)
            let sub_progress = sub_tracker.progress();
            total_weighted_progress += sub_progress * sub_info.weight;
        }

        total_weighted_progress
    }

    /// Update the current progress value
    pub fn update(&self, value: u64) {
        self.inner.current.store(value, Ordering::Release);
    }

    /// Increment the current progress value
    pub fn increment(&self, increment: u64) {
        self.inner.current.fetch_add(increment, Ordering::Release);
    }

    /// Increment the current progress to full completion
    pub fn increment_to_full(&self) {
        // Set current to total to mark as complete
        let total = self.inner.total.load(Ordering::Acquire);
        self.inner.current.store(total, Ordering::Release);
    }

    /// Check if the operation has been cancelled
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancellation_token.is_cancelled()
    }

    /// Cancel the operation
    pub fn cancel(&self) {
        self.inner.cancellation_token.cancel();
    }

    /// Create a sub-tracker that contributes to this tracker's progress
    ///
    /// # Arguments
    /// * `weight` - The weight of this sub-tracker in the parent tracker (0.0 to 1.0)
    ///   For example, if weight is 0.1, this sub-tracker contributes to 10% of the parent's progress
    /// * `description` - The description for this sub-tracker
    #[must_use]
    pub fn create_sub_tracker(&self, weight: f64, description: impl Into<String>) -> Self {
        // Clamp the weight to the range [0.0, 1.0]
        let weight = weight.clamp(0.0, 1.0);

        let description_string = description.into();
        let sub_tracker = Self {
            inner: Arc::new(ProgressTrackerInner {
                current: AtomicU64::new(0),
                total: AtomicU64::new(100),
                description: description_string.clone(),
                cancellation_token: self.inner.cancellation_token.clone(),
                sub_trackers: Mutex::new(Vec::new()),
            }),
        };

        // Add this sub-tracker to the parent's list
        {
            let mut sub_trackers = Self::lock_mutex(&self.inner.sub_trackers, "Sub-trackers");
            sub_trackers.push(SubTrackerInfo {
                tracker: Arc::clone(&sub_tracker.inner),
                weight,
            });
        }

        sub_tracker
    }

    /// Get a reference to the cancellation token
    #[must_use]
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.inner.cancellation_token
    }

    /// A cancellation future that can be awaited
    pub fn cancelled(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.inner.cancellation_token.cancelled()
    }

    /// Get the current description of this tracker
    #[must_use]
    pub fn description(&self) -> String {
        self.inner.description.clone()
    }

    /// Get all tracker information including sub-trackers
    /// Returns a vector of (`description`, `progress`, `weight`) tuples
    #[must_use]
    pub fn all_trackers(&self) -> Vec<(String, f64, f64)> {
        let mut trackers = Vec::new();

        // Add this tracker's info
        let own_description = self.inner.description.clone();
        let own_progress = self.progress();
        trackers.push((own_description, own_progress, 1.0)); // Root tracker has weight 1.0

        // Add all sub-tracker info
        let sub_trackers = Self::lock_mutex(&self.inner.sub_trackers, "Sub-trackers");
        for sub_info in sub_trackers.iter() {
            let sub_progress = Self::calculate_progress_from_inner(&sub_info.tracker);
            trackers.push((
                sub_info.tracker.description.clone(),
                sub_progress,
                sub_info.weight,
            ));

            // Recursively get trackers from sub-tracker's sub-trackers
            let sub_tracker_wrapper = ProgressTracker {
                inner: Arc::clone(&sub_info.tracker),
            };
            let mut nested_trackers = sub_tracker_wrapper.all_trackers();
            // Skip the first tracker (it's the sub-tracker itself, already added)
            if !nested_trackers.is_empty() {
                nested_trackers.remove(0);
                // Adjust weights by multiplying with parent weight
                for (description, progress, weight) in nested_trackers {
                    trackers.push((description, progress, weight * sub_info.weight));
                }
            }
        }

        trackers
    }

    /// Get a summary of all active trackers with their progress
    #[must_use]
    pub fn tracker_summary(&self) -> String {
        use std::fmt::Write;

        let trackers = self.all_trackers();
        if trackers.is_empty() {
            return "No active trackers".to_string();
        }

        let mut summary = String::new();
        for (i, (description, progress, weight)) in trackers.iter().enumerate() {
            if i > 0 {
                summary.push_str(", ");
            }
            let _ = write!(
                summary,
                "{description}: {progress:.3} (weight: {weight:.1})"
            );
        }
        summary
    }

    /// Remove a sub-tracker from this tracker's list
    ///
    /// # Arguments
    /// * `sub_tracker` - The sub-tracker to remove
    pub fn remove_sub_tracker(&self, sub_tracker: &Self) {
        let mut sub_trackers = Self::lock_mutex(&self.inner.sub_trackers, "Sub-trackers");

        // Find and remove the sub-tracker by its Arc reference
        sub_trackers.retain(|info| !Arc::ptr_eq(&info.tracker, &sub_tracker.inner));
        // No need to recalculate progress as it will be done on-demand
    }
}

impl fmt::Display for ProgressTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn format_tracker(
            tracker: &ProgressTracker,
            f: &mut fmt::Formatter<'_>,
            indent: usize,
        ) -> fmt::Result {
            let indent_str = " ".repeat(indent);
            let description = tracker.description();
            let progress = tracker.progress() * 100.0;

            writeln!(f, "{indent_str}{description}: {progress:.2}%")?;

            // Display sub-trackers
            let sub_trackers =
                ProgressTracker::lock_mutex(&tracker.inner.sub_trackers, "Sub-trackers");
            for sub_info in sub_trackers.iter() {
                let sub_tracker = ProgressTracker {
                    inner: Arc::clone(&sub_info.tracker),
                };
                format_tracker(&sub_tracker, f, indent + 2)?;
            }

            Ok(())
        }

        format_tracker(self, f, 0)
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to assert floating-point equality with a small epsilon
    fn assert_float_eq(actual: f64, expected: f64) {
        const EPSILON: f64 = 1e-6;
        assert!(
            (actual - expected).abs() < EPSILON,
            "Expected {expected}, got {actual} (difference: {})",
            (actual - expected).abs()
        );
    }

    #[test]
    fn test_basic_progress() {
        let tracker = ProgressTracker::new();
        assert_float_eq(tracker.progress(), 0.0);

        tracker.update(50);
        assert_float_eq(tracker.progress(), 0.5);

        tracker.update(100);
        assert_float_eq(tracker.progress(), 1.0);
    }

    #[test]
    fn test_custom_total() {
        let tracker = ProgressTracker::new();
        tracker.set_total(1000);
        tracker.update(250);
        assert_float_eq(tracker.progress(), 0.25);
    }

    #[test]
    fn test_increment() {
        let tracker = ProgressTracker::new();
        tracker.increment(25);
        assert_float_eq(tracker.progress(), 0.25);
        tracker.increment(25);
        assert_float_eq(tracker.progress(), 0.5);
    }

    #[test]
    fn test_cancellation() {
        let tracker = ProgressTracker::new();
        assert!(!tracker.is_cancelled());

        tracker.cancel();
        assert!(tracker.is_cancelled());
    }

    #[test]
    fn test_sub_tracker() {
        let parent = ProgressTracker::new();

        // Create sub-tracker for first 30% of work
        let sub1 = parent.create_sub_tracker(0.3, "Task 1");

        // Complete the sub-tracker
        sub1.update(100);

        // Parent should now be at 0.3 (0.3 weight * 1.0 progress)
        assert_float_eq(parent.progress(), 0.3);

        // Create sub-tracker for next 70% of work
        let sub2 = parent.create_sub_tracker(0.7, "Task 2");

        // Complete half of the sub-tracker (0.5 of sub2)
        sub2.update(50);

        // Parent should now be at:
        // (0.3 weight * 1.0 progress) + (0.7 weight * 0.5 progress) = 0.3 + 0.35 = 0.65
        assert_float_eq(parent.progress(), 0.65);
    }

    #[test]
    fn test_nested_sub_trackers() {
        let parent = ProgressTracker::new();

        // First sub-tracker: 60% of total
        let sub1 = parent.create_sub_tracker(0.6, "Main Task");

        // Sub-tracker of sub1: 50% of sub1
        let subsub = sub1.create_sub_tracker(0.5, "Sub Task");

        // Complete subsub
        subsub.update(100);

        // sub1 should be at 0.5
        assert_float_eq(sub1.progress(), 0.5);

        // parent should be at 0.3 (0.6 * 0.5)
        assert_float_eq(parent.progress(), 0.3);
    }

    #[test]
    fn test_description_functionality() {
        let tracker = ProgressTracker::new();
        assert_eq!(tracker.description(), "Tracker");

        let tracker2 = ProgressTracker::new_with_description("Processing data");
        assert_eq!(tracker2.description(), "Processing data");

        let tracker3 = ProgressTracker::new_with_description("Finalizing");
        assert_eq!(tracker3.description(), "Finalizing");
    }

    #[test]
    fn test_set_description_and_total() {
        let tracker = ProgressTracker::new_with_description("Custom Tracker");
        tracker.set_total(500);
        assert_eq!(tracker.description(), "Custom Tracker");
        assert_eq!(tracker.total(), 500);
    }

    #[test]
    fn test_sub_tracker_with_description() {
        let parent = ProgressTracker::new_with_description("Main task");

        let sub = parent.create_sub_tracker(0.5, "Reading files");
        assert_eq!(sub.description(), "Reading files");
        assert_eq!(parent.description(), "Main task");

        // Descriptions are immutable - create a new tracker if we need a different description
        let sub2 = parent.create_sub_tracker(0.3, "Processing files");
        assert_eq!(sub2.description(), "Processing files");
    }

    #[test]
    fn test_all_trackers() {
        let parent = ProgressTracker::new_with_description("Main task");

        let sub1 = parent.create_sub_tracker(0.6, "Reading files");
        sub1.update(50); // 50% of sub1

        let sub2 = parent.create_sub_tracker(0.4, "Writing files");
        sub2.update(75); // 75% of sub2

        // Parent progress should be: 0.6 * 0.5 + 0.4 * 0.75 = 0.3 + 0.3 = 0.6
        let expected_parent_progress = 0.6;

        let trackers = parent.all_trackers();
        assert_eq!(trackers.len(), 3);

        // Check main task
        assert_eq!(trackers[0].0, "Main task");
        assert_float_eq(trackers[0].1, expected_parent_progress); // Calculated from sub-trackers
        assert_float_eq(trackers[0].2, 1.0); // weight 1.0

        // Check sub-trackers
        assert_eq!(trackers[1].0, "Reading files");
        assert_float_eq(trackers[1].1, 0.5); // 0.5 progress
        assert_float_eq(trackers[1].2, 0.6); // weight 0.6

        assert_eq!(trackers[2].0, "Writing files");
        assert_float_eq(trackers[2].1, 0.75); // 0.75 progress
        assert_float_eq(trackers[2].2, 0.4); // weight 0.4
    }

    #[test]
    fn test_nested_trackers() {
        let parent = ProgressTracker::new_with_description("Main task");

        let sub1 = parent.create_sub_tracker(0.8, "Phase 1");
        let subsub = sub1.create_sub_tracker(0.5, "Sub-phase 1.1");
        subsub.update(100); // Complete sub-sub-tracker

        let trackers = parent.all_trackers();
        assert!(trackers.len() >= 3);

        // Should include main, sub, and sub-sub trackers
        let tracker_descriptions: Vec<&String> = trackers
            .iter()
            .map(|(description, _, _)| description)
            .collect();
        assert!(tracker_descriptions.contains(&&"Main task".to_string()));
        assert!(tracker_descriptions.contains(&&"Phase 1".to_string()));
        assert!(tracker_descriptions.contains(&&"Sub-phase 1.1".to_string()));
    }

    #[test]
    fn test_tracker_summary() {
        let parent = ProgressTracker::new_with_description("Main");
        parent.update(10);

        let sub = parent.create_sub_tracker(0.5, "Sub");
        sub.update(20);

        let summary = parent.tracker_summary();
        assert!(summary.contains("Main"));
        assert!(summary.contains("Sub"));
        assert!(summary.contains("0.100"));
        assert!(summary.contains("0.200"));
    }

    #[test]
    fn test_concurrent_updates() {
        use std::{sync::Arc as StdArc, thread};

        let tracker = StdArc::new(ProgressTracker::new_with_description("Concurrent Test"));
        let mut handles = vec![];

        // Spawn multiple threads that update progress concurrently
        for _i in 0..10 {
            let tracker_clone = StdArc::clone(&tracker);
            let handle = thread::spawn(move || {
                for _j in 0..10 {
                    tracker_clone.increment(1);
                    // Small delay to increase chance of concurrent access
                    thread::sleep(std::time::Duration::from_millis(1));
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state is consistent
        assert_float_eq(tracker.progress(), 1.0); // 10 threads * 10 increments = 100% progress

        // Description should remain unchanged since descriptions are immutable
        let final_description = tracker.description();
        assert_eq!(final_description, "Concurrent Test");
    }

    #[test]
    fn test_concurrent_sub_tracker_creation() {
        use std::{sync::Arc as StdArc, thread};

        let parent = StdArc::new(ProgressTracker::new_with_description("Concurrent Parent"));
        let mut handles = vec![];

        // Spawn multiple threads that create sub-trackers concurrently
        for i in 0..5 {
            let parent_clone = StdArc::clone(&parent);
            let handle = thread::spawn(move || {
                let sub = parent_clone.create_sub_tracker(0.1, format!("Sub-{i}"));
                sub.update(50); // Each sub-tracker at 0.5 progress
                // Descriptions are immutable, so no need to update them
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify that all sub-trackers were created and contributed to progress
        let trackers = parent.all_trackers();
        assert!(trackers.len() >= 6); // Parent + 5 sub-trackers

        // Parent should have some progress from sub-trackers (5 * 0.1 * 0.5 = 0.25)
        assert_float_eq(parent.progress(), 0.25);
    }

    #[test]
    fn test_sub_tracker_removal() {
        let parent = ProgressTracker::new();

        // Create some sub-trackers
        let sub1 = parent.create_sub_tracker(0.4, "Sub-1");
        let sub2 = parent.create_sub_tracker(0.6, "Sub-2");

        // Update progress
        sub1.update(100); // 100% of 0.4 weight = 0.4 of parent
        sub2.update(50); // 50% of 0.6 weight = 0.3 of parent

        // Total parent progress should be 0.7
        assert_float_eq(parent.progress(), 0.7);

        // Remove sub1
        parent.remove_sub_tracker(&sub1);

        // Now parent should only reflect sub2's contribution: 0.5 of 0.6 weight = 0.3
        assert_float_eq(parent.progress(), 0.3);

        // Check trackers - should only include parent and sub2
        let trackers = parent.all_trackers();
        let tracker_descriptions: Vec<&String> = trackers
            .iter()
            .map(|(description, _, _)| description)
            .collect();
        assert!(!tracker_descriptions.contains(&&"Sub-1".to_string()));
        assert!(tracker_descriptions.contains(&&"Sub-2".to_string()));

        // Remove sub2
        parent.remove_sub_tracker(&sub2);

        // Parent should now have 0.0 progress (no sub-trackers)
        assert_float_eq(parent.progress(), 0.0);
    }

    #[test]
    fn test_deep_nesting() {
        let root = ProgressTracker::new_with_description("Root");

        // Create a deep hierarchy: Root -> L1 -> L2 -> L3
        let l1 = root.create_sub_tracker(1.0, "Level 1");
        let l2 = l1.create_sub_tracker(1.0, "Level 2");
        let l3 = l2.create_sub_tracker(1.0, "Level 3");

        // Update the deepest level
        l3.update(50);

        // All levels should reflect the progress
        assert_float_eq(l3.progress(), 0.5);
        assert_float_eq(l2.progress(), 0.5);
        assert_float_eq(l1.progress(), 0.5);
        assert_float_eq(root.progress(), 0.5);

        // Check that all descriptions are set correctly
        let trackers = root.all_trackers();
        let tracker_descriptions: Vec<&String> = trackers
            .iter()
            .map(|(description, _, _)| description)
            .collect();
        assert!(tracker_descriptions.contains(&&"Root".to_string()));
        assert!(tracker_descriptions.contains(&&"Level 1".to_string()));
        assert!(tracker_descriptions.contains(&&"Level 2".to_string()));
        assert!(tracker_descriptions.contains(&&"Level 3".to_string()));
    }

    #[test]
    fn test_sub_tracker_creation() {
        let parent = ProgressTracker::new_with_description("Parent");

        // Create sub-trackers
        let sub1 = parent.create_sub_tracker(0.5, "Sub1");
        let _sub2 = parent.create_sub_tracker(0.5, "Sub2"); // Create another sub-tracker to verify count

        // Verify sub-trackers are tracked by the parent
        let trackers = parent.all_trackers();
        assert_eq!(trackers.len(), 3); // Parent + 2 sub-trackers

        // Create a sub-sub-tracker
        let _subsub = sub1.create_sub_tracker(0.5, "SubSub"); // Add a sub-tracker to sub1

        // Verify the sub-sub-tracker is tracked by sub1
        let sub1_trackers = sub1.all_trackers();
        assert_eq!(sub1_trackers.len(), 2); // Sub1 + subsub
    }

    #[test]
    fn test_dynamic_progress_updates_with_deep_nesting() {
        // Create a deeply nested tracker hierarchy
        let root = ProgressTracker::new_with_description("Root Task");
        root.set_total(1000);

        // Level 1 - three sub-tasks with different weights
        let task_a = root.create_sub_tracker(0.5, "Task A");
        task_a.set_total(800);

        let task_b = root.create_sub_tracker(0.3, "Task B");
        task_b.set_total(600);

        let task_c = root.create_sub_tracker(0.2, "Task C");
        task_c.set_total(400);

        // Level 2 - nested under task_a
        let task_a1 = task_a.create_sub_tracker(0.6, "Task A.1");
        task_a1.set_total(300);

        let task_a2 = task_a.create_sub_tracker(0.4, "Task A.2");
        task_a2.set_total(500);

        // Level 3 - nested under task_a1
        let task_a1_i = task_a1.create_sub_tracker(0.7, "Task A.1.i");
        task_a1_i.set_total(200);

        let task_a1_ii = task_a1.create_sub_tracker(0.3, "Task A.1.ii");
        task_a1_ii.set_total(100);

        // Start updating progress from the bottom up
        task_a1_i.update(100); // 50% of Task A.1.i
        assert_float_eq(task_a1_i.progress(), 0.5);

        // This should propagate up
        // Task A.1 should be (0.7 * 0.5 + 0.3 * 0.0) = 0.35
        assert_float_eq(task_a1.progress(), 0.35);

        // Task A should be (0.6 * 0.35 + 0.4 * 0.0) = 0.21
        assert_float_eq(task_a.progress(), 0.21);

        // Root should be (0.5 * 0.21 + 0.3 * 0.0 + 0.2 * 0.0) = 0.105
        assert_float_eq(root.progress(), 0.105);

        // Now update task_c directly (level 1)
        task_c.update(200); // 50% of Task C

        // Root should now be (0.5 * 0.21 + 0.3 * 0.0 + 0.2 * 0.5) = 0.105 + 0.1 = 0.205
        assert_float_eq(root.progress(), 0.205);

        // Complete task_a1_ii (level 3)
        task_a1_ii.update(100); // 100% of Task A.1.ii

        // Task A.1 should now be (0.7 * 0.5 + 0.3 * 1.0) = 0.35 + 0.3 = 0.65
        assert_float_eq(task_a1.progress(), 0.65);

        // Task A should now be (0.6 * 0.65 + 0.4 * 0.0) = 0.39
        assert_float_eq(task_a.progress(), 0.39);

        // Root should now be (0.5 * 0.39 + 0.3 * 0.0 + 0.2 * 0.5) = 0.195 + 0.1 = 0.295
        assert_float_eq(root.progress(), 0.295);

        // Now complete task_b directly (level 1) - skipping all its potential subtasks
        task_b.update(600); // 100% of Task B

        // Root should now be (0.5 * 0.39 + 0.3 * 1.0 + 0.2 * 0.5) = 0.195 + 0.3 + 0.1 = 0.595
        assert_float_eq(root.progress(), 0.595);

        // Finally, complete all remaining tasks
        task_a1_i.update(200); // 100% of Task A.1.i
        task_a2.update(500); // 100% of Task A.2
        task_c.update(400); // 100% of Task C

        // Everything should be at 1.0
        assert_float_eq(task_a1.progress(), 1.0);
        assert_float_eq(task_a.progress(), 1.0);
        assert_float_eq(task_b.progress(), 1.0);
        assert_float_eq(task_c.progress(), 1.0);
        assert_float_eq(root.progress(), 1.0);

        // Check descriptions are set correctly
        assert_eq!(root.description(), "Root Task");
        assert_eq!(task_a.description(), "Task A");
        assert_eq!(task_a1.description(), "Task A.1");
        assert_eq!(task_a1_i.description(), "Task A.1.i");
    }
}
