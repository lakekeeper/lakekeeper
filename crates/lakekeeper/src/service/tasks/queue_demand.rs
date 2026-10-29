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
}

impl TaskQueueDemand {
    /// Workers the tasks need: due, heartbeat-expired and running tasks.
    #[must_use]
    pub fn demand(&self) -> i64 {
        self.due + self.heartbeat_expired + self.running
    }
}

/// Lower case letters, digits and `-`, not empty.
pub(crate) fn is_valid_demand_class_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}
