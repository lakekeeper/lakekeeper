use chrono::{DateTime, Utc};

use super::InputTrait;
use crate::{service::task_queue::tabular_expiration_queue::TabularExpirationInput, WarehouseId};

// #[cfg(test)]
// mod test {
//     use sqlx::PgPool;
//
//     use super::super::test::setup;
//     use crate::{
//         implementations::postgres::task_queues::TabularExpirationQueue,
//         service::task_queue::{
//             tabular_expiration_queue::TabularExpirationInput, TaskFilter, TaskQueue,
//             TaskQueueConfig,
//         },
//     };
//
//     #[sqlx::test]
//     async fn test_queue_expiration_queue_task(pool: PgPool) {
//         let config = TaskQueueConfig::default();
//         let (pg_queue, warehouse_ident) = setup(pool, config).await;
//         let queue = super::TabularExpirationQueue { pg_queue };
//         let input = TabularExpirationInput {
//             tabular_id: uuid::Uuid::new_v4(),
//             warehouse_ident,
//             tabular_type: crate::api::management::v1::TabularType::Table,
//             purge: false,
//             expire_at: chrono::Utc::now(),
//         };
//         queue.enqueue(input.clone()).await.unwrap();
//         queue.enqueue(input.clone()).await.unwrap();
//
//         let task = queue
//             .pick_new_task()
//             .await
//             .unwrap()
//             .expect("There should be a task");
//
//         assert_eq!(task.warehouse_ident, input.warehouse_ident);
//         assert_eq!(task.tabular_id, input.tabular_id);
//         assert_eq!(task.tabular_type, input.tabular_type);
//         assert_eq!(
//             task.deletion_kind,
//             crate::implementations::postgres::DeletionKind::Default.into()
//         );
//
//         let task = queue.pick_new_task().await.unwrap();
//         assert!(
//             task.is_none(),
//             "There should only be one task, idempotency didn't work."
//         );
//     }
//
//     #[sqlx::test]
//     async fn test_cancel_pending_tasks(pool: PgPool) {
//         let config = TaskQueueConfig::default();
//         let (pg_queue, warehouse_ident) = setup(pool.clone(), config.clone()).await;
//         let queue = TabularExpirationQueue { pg_queue };
//
//         let input = TabularExpirationInput {
//             tabular_id: uuid::Uuid::new_v4(),
//             warehouse_ident,
//             tabular_type: crate::api::management::v1::TabularType::Table,
//             purge: false,
//             expire_at: chrono::Utc::now(),
//         };
//         queue.enqueue(input.clone()).await.unwrap();
//
//         queue
//             .cancel_pending_tasks(TaskFilter::WarehouseId(warehouse_ident))
//             .await
//             .unwrap();
//
//         let task = queue.pick_new_task().await.unwrap();
//         assert!(task.is_none(), "There should be no tasks");
//     }
// }
