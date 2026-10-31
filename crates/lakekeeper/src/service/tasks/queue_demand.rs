//! How many tasks of a queue wait or run, per demand class, for autoscalers.

use super::{TaskQueueName, TaskResourceDemandRange};

/// A named range of resource demands that one kind of worker of a queue picks, such as a size
/// class of workers. Autoscalers ask for the demand of a class by its name.
#[derive(Debug, Clone, PartialEq, Eq, typed_builder::TypedBuilder)]
pub struct TaskDemandClass {
    /// Lower case letters, digits and `-`.
    #[builder(setter(into))]
    pub name: String,
    /// The resource demands of the tasks the class's workers pick.
    pub resource_demand: TaskResourceDemandRange,
}

/// Which tasks [`crate::service::CatalogTaskOps::task_queue_demand`] counts.
#[derive(Debug, Clone, typed_builder::TypedBuilder)]
pub struct TaskQueueDemandRequest<'a> {
    pub queue_name: &'a TaskQueueName,
    /// Earlier names of the queue; their tasks are counted too.
    #[builder(default)]
    pub legacy_queue_names: &'a [&'a TaskQueueName],
    /// Used only if no task configuration of the queue is stored.
    pub default_max_time_since_last_heartbeat: chrono::Duration,
    /// Count only tasks whose resource demand lies in this range. Default: every task.
    #[builder(default)]
    pub resource_demand: TaskResourceDemandRange,
}

/// Tasks of a queue in a resource demand range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TaskQueueDemand {
    /// Scheduled and due: what a pick can take now.
    pub due: i64,
    /// Running or asked to stop, with a heartbeat older than the queue's timeout. The next pick
    /// of the queue fails such an attempt.
    pub heartbeat_expired: i64,
    /// Running or asked to stop, with a live heartbeat.
    pub running: i64,
    /// Since when the longest waiting task can be taken: the `scheduled_for` of a due task, or
    /// when the heartbeat of a heartbeat-expired task expired. `None` if no task is either.
    pub oldest_due_since: Option<chrono::DateTime<chrono::Utc>>,
}

impl TaskQueueDemand {
    /// Workers the tasks need: due, heartbeat-expired and running tasks.
    #[must_use]
    pub fn demand(&self) -> i64 {
        self.due + self.heartbeat_expired + self.running
    }

    /// How long the longest waiting due or heartbeat-expired task has waited at `now`, in whole
    /// seconds. `None` if no task is either.
    #[must_use]
    pub fn oldest_due_seconds(&self, now: chrono::DateTime<chrono::Utc>) -> Option<i64> {
        self.oldest_due_since
            .map(|since| (now - since).num_seconds().max(0))
    }
}

/// Lower case letters, digits and `-`, not empty.
pub(crate) fn is_valid_demand_class_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_oldest_due_task_waited_since_it_could_be_taken() {
        let since = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        let demand = TaskQueueDemand {
            due: 1,
            oldest_due_since: Some(since),
            ..TaskQueueDemand::default()
        };
        for (now, expected) in [
            (since + chrono::Duration::milliseconds(90_999), 90),
            (since, 0),
            // The database's clock is ahead.
            (since - chrono::Duration::seconds(2), 0),
        ] {
            assert_eq!(demand.oldest_due_seconds(now), Some(expected), "{now}");
        }
        assert_eq!(TaskQueueDemand::default().oldest_due_seconds(since), None);
    }
}
