---
title: "Optimize Workers"
description: "Run Lakekeeper Plus optimize workers: with the Helm chart or on your own, how a run gets its memory, cores and spill disk, how size classes match tasks to worker pods, and how to stop a worker without losing work."
source: "https://docs.lakekeeper.io/docs/latest/optimize-workers/"
---

# Optimize Workers { #optimize-workers .lkp }

[Optimize](./optimize.md) runs in Lakekeeper Plus processes that set `LAKEKEEPER__TASK_OPTIMIZE_WORKERS` above 0. The default is 0: Lakekeeper schedules optimize runs, but they wait until a worker takes them. Compaction needs a lot of memory, CPU and disk, so we recommend running optimize workers in their own pods, not in the pods that serve the API.

We recommend enabling [Remove Orphan Files](./table-maintenance.md#remove-orphan-files) on every warehouse that runs optimize. A run that is stopped before it commits leaves the files it wrote.

## With the Helm Chart { #helm-chart }

The [Lakekeeper Helm chart](https://github.com/lakekeeper/lakekeeper-charts/tree/main/charts/lakekeeper) 0.14.0 or later runs optimize workers as Kubernetes Jobs that [KEDA](https://keda.sh/) starts only while optimize tasks wait. Older chart versions ignore `optimizeWorkers`. [Install KEDA](https://keda.sh/docs/latest/deploy/) in the cluster first, then turn on `optimizeWorkers`:

```yaml
lakekeeper:
  edition: plus

optimizeWorkers:
  enabled: true
  sizeClasses:
    - name: small
      resources:
        cpu: "2"
        memory: 8Gi
      keda:
        maxJobs: 4
```

The chart creates one KEDA ScaledJob per size class. KEDA starts one Job for each optimize task of the class that waits or runs, up to `keda.maxJobs` of the class. Each Job runs one optimize worker without the HTTP API, with the `resources` of its class as CPU and memory request and limit. A Job ends after it ran no task for `optimizeWorkers.keda.idleExitAfter` (180 seconds by default), and takes no new task after `optimizeWorkers.keda.maxLifetime` (1 hour by default). Each Job opens up to 2 read and 2 write database connections (`databasePoolConnections`).

Choose these values:

- **`sizeClasses`**: one class `small` with 2 CPUs, 8 GiB and `keda.maxJobs: 2` by default. `cpu` must be a whole number. See [Size Classes](#size-classes).
- **`spill`**: the disk at `/tmp` where compaction writes spill files. Each Job gets `spill.sizePerCpu` (4 GiB by default) for each CPU of its class. The chart sets the spill limit of Lakekeeper a little below this size. With `spill.type: local` (the default), the disk is an `emptyDir` on the node, and the Job requests its size as `ephemeral-storage`, so it starts only on a node with enough free disk. With `spill.type: pvc`, each Job gets its own PersistentVolumeClaim, which Kubernetes deletes when KEDA deletes the finished Job. We recommend `pvc` if your nodes have small disks, for example the 20 GiB default of EKS node groups and Karpenter, or GKE Autopilot. Set `spill.pvc.storageClassName` if your cluster has no default storage class. A size class can set its own `spill.type` and `spill.pvc`, and a fixed `spill.size` in place of the size per CPU.
- **`drainTimeout`**: `0s` by default, see [Stopping a Worker](#stopping-a-worker). The chart refuses a `terminationGracePeriodSeconds` (60 by default) below the drain timeout plus 30 seconds.
- **`nodeSelector`, `tolerations` and `affinity`**: place the Jobs, for all classes or per class. The chart also sets the annotations that keep Karpenter and the cluster autoscaler from removing the node of a running Job.

The Jobs use `catalog.config`, `catalog.extraEnv`, `catalog.extraEnvFrom`, `catalog.extraVolumes` and `catalog.extraVolumeMounts` of the API pods, except the variables that the chart sets for workers, such as the task worker counts and the drain timeout. The worker container does not get a mount at `/tmp` from `catalog.extraVolumeMounts`, because its `/tmp` is the spill disk. Variables in `optimizeWorkers.extraEnv` apply only to the workers and override `catalog.extraEnv`.

The workers have their own pod settings:

- **Labels and annotations:** `optimizeWorkers.podLabels` and `optimizeWorkers.podAnnotations`. Labels in `additionalLabels` go on all pods. For example, put the label `azure.workload.identity/use: "true"` for Azure Workload Identity there.
- **Sidecars:** add them in `optimizeWorkers.extraInitContainers` as native sidecars (`restartPolicy: Always`, Kubernetes 1.29 or later). A regular sidecar keeps the Job from completing. With the Vault Agent Injector, set `vault.hashicorp.com/agent-pre-populate-only: "true"` in `optimizeWorkers.podAnnotations`. Istio 1.27 and Linkerd 2.20 or later inject native sidecars by default.

The chart's README lists all other values. Then turn on optimize for a warehouse, see [Turn It On](./optimize.md#turn-it-on).

## Without the Helm Chart

If you run optimize workers yourself, set `LAKEKEEPER__OPTIMIZE__AVAILABLE=true` on the API pods and on every optimize worker. We also recommend:

- **One optimize worker per pod** (`LAKEKEEPER__TASK_OPTIMIZE_WORKERS=1`) for large tables. If one run uses more memory than the pod has, the whole container is killed, and every run in it must start again. Keep the setting at 0 on the API pods.
- **A memory limit, and a memory request of the same size.** Compaction plans with the memory limit, so the node must have this memory free. Without a limit, compaction plans with the memory of the whole node, and the worker warns at startup.
- **A disk for spill files.** Spill files go to the temporary directory: `TMPDIR`, or `/tmp` if it is not set. Mount a disk-backed `emptyDir` with a `sizeLimit` there. Lakekeeper cannot see this limit, so set `LAKEKEEPER__OPTIMIZE__SPILL_DISK_LIMIT_BYTES` a little below it. Do not use `medium: Memory`: it counts against the memory limit.
- **The annotations `karpenter.sh/do-not-disrupt: "true"` and `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`**, so that Karpenter and the cluster autoscaler do not remove the node while a run writes.

To start workers only while the queue has tasks that wait or run, see [Task Workers on Kubernetes](./task-workers.md). Use the queue `optimize` with `targetValue: "1"`. With size classes, create one ScaledJob per class: its workers set `LAKEKEEPER__OPTIMIZE__SIZE_CLASS` to the class name, and its demand URL ends with `?class=<name>`.

## Memory, Cores and Spill Disk

Each optimize worker runs one task at a time. A process divides its memory, cores and spill disk equally between its optimize workers. By default, it uses:

- **Memory:** the memory limit of the container. Compaction keeps most of it for sorting and writing, and writes to the spill disk when that is not enough.
- **Cores:** the CPU limit of the container, rounded down, but at most one core per 4 GiB of memory.
- **Spill disk:** 100 GiB, in the temporary directory (`TMPDIR`, or `/tmp` if it is not set). To spill to another disk, set `TMPDIR` to a directory on it.

To change these values, see [Optimize Settings](./configuration.md#optimize-settings). At startup, the worker logs a line that starts with `Optimize:` and shows the memory, cores and spill disk of each run.

## Size Classes

Lakekeeper estimates how many cores and how much memory each optimize task needs. With size classes, you run worker pods of a few sizes, for example `small` and `large`, and each pod takes only the tasks of its class. Small tasks then do not take up large pods, and large tasks get enough memory.

We recommend starting with one class, as the Helm chart does by default. Add a larger class when the task log asks for more memory, or when your largest tables take a long time to optimize.

- Set the classes in `optimizeWorkers.sizeClasses` of the Helm chart. Without the chart, see `LAKEKEEPER__OPTIMIZE__SIZE_CLASSES` and `LAKEKEEPER__OPTIMIZE__SIZE_CLASS` in [Optimize Settings](./configuration.md#optimize-settings).
- Every class must have the same memory per core, and each class needs a different number of cores. We recommend at least 4 GiB per core: with less, a run uses fewer cores than the class has.
- A task larger than every class runs in the largest class. If Lakekeeper finds that a task is larger than its class, the task moves to a larger class.
- If a worker stops responding during a task, for example because it ran out of memory, the task runs again in a larger class. If the pod was `OOMKilled`, add a larger class, or give the largest class more cores and memory.
- Tables with many equality deletes, for example from Flink upserts, need a lot of memory. If a run of the largest class cannot hold them, it fails before it writes anything, and the [task log](./optimize.md#see-the-results) names the memory it needs. Add a larger class, or apply the deletes with another engine.

## Stopping a Worker

When a worker gets a stop signal (SIGTERM or SIGINT), a running compaction starts no new work and commits the files it already started. A new run continues with the rest right away. The worker waits at most `LAKEKEEPER__TASK_DRAIN_TIMEOUT` (`drainTimeout` in the Helm chart) for this commit. With the default of 0 seconds, the work that is not committed is lost, and compaction continues about 2 minutes later. Neither case counts as a failure. A stopped manifest rewrite counts as a failed attempt and is tried again. A worker that reaches `LAKEKEEPER__TASK_MAX_LIFETIME` waits until the run has committed.

Set a drain timeout if your nodes give pods time to stop, for example for node upgrades or `kubectl drain`:

- Keep the drain timeout below the time your nodes give a pod before they kill it. 300 seconds is enough for most tables. If your tables have data files larger than 1 GiB, set 600 seconds if your nodes allow it.
- Set the pod's `terminationGracePeriodSeconds` to the drain timeout plus about 30 seconds, plus the time of a `preStop` hook if the pod has one.
- Keep the drain timeout at 0 on spot or preemptible nodes. These nodes can go away before the drain ends, and a worker that is killed while it drains counts as a failed attempt.

See [Stopping Task Workers](./configuration.md#stopping-task-workers) for how the drain works for all queues.
