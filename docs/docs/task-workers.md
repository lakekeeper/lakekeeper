---
description: "Run Lakekeeper task workers as Kubernetes Jobs that KEDA starts only while tasks wait: setup, database connections, leftover files and alerting."
---

# Task Workers on Kubernetes

Lakekeeper runs background work in task-queue workers, for example purging the files of dropped tables and, in Lakekeeper Plus, expiring snapshots and removing orphan files. By default every Lakekeeper pod runs these workers next to the HTTP API. On Kubernetes you can move this work to worker pods that [KEDA](https://keda.sh/) starts only while tasks wait. The API pods then stay free of heavy work, and the cluster can remove nodes when there is nothing to do.

## How It Works

1. The API pods run no workers of the queue. On their metrics port they report the [task queue demand](./monitoring.md#task-queue-demand): how many tasks of the queue wait or run.
2. A KEDA ScaledJob reads the demand and starts worker Jobs.
3. Each Job runs Lakekeeper without the HTTP API (`LAKEKEEPER__SERVE_HTTP_API=false`). It takes tasks until it had no task for `LAKEKEEPER__TASK_IDLE_EXIT_AFTER` or reached `LAKEKEEPER__TASK_MAX_LIFETIME`, and then exits with code 0, so the Job completes. See [Stopping Task Workers](./configuration.md#stopping-task-workers).

We recommend one ScaledJob per queue. Set the queue's `LAKEKEEPER__TASK_*_WORKERS` to `0` on the API pods, and on the workers set the worker counts of all other queues to `0`.

## Example

A ScaledJob for the `tabular_purge` queue:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledJob
metadata:
  name: lakekeeper-tabular-purge
spec:
  maxReplicaCount: 5
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 10
  jobTargetRef:
    backoffLimit: 0
    template:
      spec:
        restartPolicy: Never
        containers:
          - name: worker
            image: quay.io/lakekeeper/catalog:<version>
            args: ["serve"]
            env:
              # Also add the database, secret and storage settings of the API pods.
              - name: LAKEKEEPER__SERVE_HTTP_API
                value: "false"
              - name: LAKEKEEPER__TASK_TABULAR_PURGE_WORKERS
                value: "2"
              - name: LAKEKEEPER__TASK_SOFT_DELETION_WORKERS
                value: "0"
              - name: LAKEKEEPER__TASK_LOG_CLEANUP_WORKERS
                value: "0"
              - name: LAKEKEEPER__TASK_IDLE_EXIT_AFTER
                value: "180s"
              - name: LAKEKEEPER__TASK_MAX_LIFETIME
                value: "3600s"
  triggers:
    - type: metrics-api
      metadata:
        url: "http://<service>.<namespace>.svc:9000/task-queue/tabular_purge/demand"
        valueLocation: "demand"
        targetValue: "2"
```

`targetValue` is the number of tasks one worker pod runs at the same time, here `LAKEKEEPER__TASK_TABULAR_PURGE_WORKERS`. With `restartPolicy: Never` and `backoffLimit: 0`, Kubernetes does not restart a worker that failed. The task queue tries the task again, and KEDA starts a new Job while tasks wait. [Task Queue Demand](./monitoring.md#task-queue-demand) lists the queue names and further recommended settings.

## Size Classes { .lkp }

The demand endpoint also accepts `?class=<name>`. It then counts only the tasks of one size class of the queue, so that each class can have its own ScaledJob with pods of a matching size. For a class that the queue does not have, the endpoint answers `404`.

<!-- TODO: describe the Lakekeeper Plus size-class settings and which queues use them once they are merged. -->

## Database Connections

Each Lakekeeper pod opens at most `LAKEKEEPER__PG_READ_POOL_CONNECTIONS` plus `LAKEKEEPER__PG_WRITE_POOL_CONNECTIONS` connections to the database (default 10 plus 5). Worker pods count too. Plan the connection limit of your database for all API pods plus the `maxReplicaCount` of every ScaledJob, each pod with its own pools. A worker pod runs only a few tasks at the same time, so we recommend smaller pools on worker pods than on API pods.

## Leftover Files

A worker that is stopped in the middle of a task, for example on a spot node, can leave files in storage that no table references. We recommend:

- enabling [Remove Orphan Files](./table-maintenance.md#remove-orphan-files) for these warehouses;
- on S3, a bucket lifecycle rule that aborts incomplete multipart uploads, for example after 7 days. Remove orphan files cannot see these uploads, because they are not objects yet.

## Alerting

A task waits as long as no worker takes it, for example when KEDA does not run, the ScaledJob reached `maxReplicaCount`, or worker pods cannot be scheduled. We recommend an alert on the age of the oldest due task of each queue, for example when it is older than one hour. This query returns the age in seconds; you can run it as a custom query of `postgres_exporter` or CloudNativePG (see [Database Monitoring](./monitoring.md#database-postgres-monitoring)):

```sql
SELECT queue_name, EXTRACT(EPOCH FROM now() - MIN(scheduled_for)) AS oldest_due_seconds
FROM task
WHERE status = 'scheduled' AND scheduled_for <= now()
GROUP BY queue_name;
```

If you set `LAKEKEEPER__PG_SCHEMA`, write the schema before the table name.
