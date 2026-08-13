# Remote Partitioning Demo — and why PR #5448's null-lock branch matters

Two runnable pieces:

1. **This 2-process demo** (`remote-partitioning-demo/`) — a real manager/worker split over
   RabbitMQ + a shared Postgres. Needs a broker + DB, so it is **not** runnable offline.
2. **A deterministic race test that IS runnable in this repo, offline:**
   `spring-batch-core/src/test/java/org/springframework/batch/core/repository/dao/jdbc/RemoteStopVersionRaceTests.java`
   — reproduces the exact cross-JVM optimistic-lock race the semaphore cannot cover.
   Run: `./mvnw -o -pl spring-batch-core test -Dtest=RemoteStopVersionRaceTests`

## The two remote models

| | Remote **Partitioning** | Remote **Chunking** |
|---|---|---|
| sent to workers | a whole **StepExecution** (a partition) | individual **item chunks** (data) |
| read happens on | each **worker** | the **manager** (single reader) |
| step's `execute()` runs on | **worker JVM** | manager owns the step |

Partitioning is the clearest case for the review: the *entire* step lifecycle
(`AbstractStep.execute()`, incl. `stepExecutionLocks.put/remove`) runs on the worker JVM.

## Roles & message flow

```
 MANAGER JVM                              WORKER JVM (separate process)
 ┌─────────────────────────┐             ┌──────────────────────────────┐
 │ Job → managerStep       │             │ StepExecutionRequestHandler  │
 │  (PartitionStep)        │  requests   │   getStepExecution(id)       │
 │  split → N StepExec     │──queue─────▶│   step.execute(stepExec) ◀───│── execute() + lock map HERE
 │  PartitionHandler       │             │   (chunk txns, updates)      │
 │  aggregate results      │◀─queue──────│   send back StepExecution    │
 │  JobOperator.stop()     │  replies    │                              │
 └─────────────────────────┘             └──────────────────────────────┘
        │        ▲
        │        │  both write the SAME BATCH_STEP_EXECUTION row
        ▼        │
     ┌──────────────────┐
     │   Shared DB      │
     └──────────────────┘
```

Key code (`StepExecutionRequestHandler.handle`, runs on the worker JVM):

```java
StepExecution stepExecution = jobRepository.getStepExecution(stepExecutionId); // load from shared DB
Step step = stepLocator.getStep(stepName);
step.execute(stepExecution);   // execute() (and the lock map) live on the worker JVM
```

## Why the "null lock branch" is exactly this picture

`AbstractStep.callUnderLock` serialises against `stepExecutionLocks.get(id)`, a map
populated inside `execute()`. In remote partitioning:

- `execute()` runs on the **worker JVM** → the semaphore exists only in the **worker's** map.
- `stop` goes through the **manager's** `JobOperator`; the manager's map has no entry
  (it never ran `execute()` for that partition) → `getStepExecutionLock` returns **null**
  → the "Not executing in this JVM" branch → the stop update runs **unlocked**.

A semaphore cannot cross JVMs anyway. What used to absorb this cross-JVM race was
`stepExecutionDao.synchronizeStatus()` re-reading VERSION in
`SimpleJobRepository.update(StepExecution)`'s stopping branch — the line this PR removed.
`RemoteStopVersionRaceTests` proves both halves: without it the stop update throws
`OptimisticLockingFailureException`; restoring it reconciles the version and succeeds.

## Running with Spring Boot

The project is one Spring Boot app (`DemoApplication`) with three profiles. A Maven
wrapper is bundled, so `./mvnw` works without a system Maven install. Spring Boot 4.0.x
(which brings Spring Batch 6.0.x) is downloaded on first build — the **first run needs
internet**; after that it is cached.

### A) LOCAL partitioning — no broker, no external DB (✅ verified end-to-end)

Single JVM, in-memory H2, partitions run on a thread pool. Nothing to install.

```bash
cd remote-partitioning-demo
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

Expected output (partitions run concurrently, then the job completes and the app exits):

```
[SimpleAsyncTaskExecutor-1] processing workerStep:partition0
[SimpleAsyncTaskExecutor-2] processing workerStep:partition1
[SimpleAsyncTaskExecutor-3] processing workerStep:partition2
Job: [localPartitionedJob] ... status: [COMPLETED]
Launched localPartitionedJob, status = COMPLETED, id = 1
```

Config: `LocalPartitioningConfiguration` (`TaskExecutorPartitionHandler` via
`.taskExecutor(...)`) + `application-local.yml` (H2). Because every worker StepExecution
runs in THIS JVM, a stop here takes the semaphore **locked** branch — the case PR #5448
fixes directly.

### B) REMOTE partitioning — real manager/worker split (needs RabbitMQ + Postgres)

```bash
cd remote-partitioning-demo
docker compose up -d                 # rabbitmq + postgres

# terminal 1 — worker(s)
./mvnw spring-boot:run -Dspring-boot.run.profiles=worker
# terminal 2 — another worker (optional)
./mvnw spring-boot:run -Dspring-boot.run.profiles=worker
# terminal 3 — manager (launches the job)
./mvnw spring-boot:run -Dspring-boot.run.profiles=manager
```

`@Profile("manager")` / `@Profile("worker")` guards keep each process to its own half, so
the duplicate `requests()`/`replies()` channel beans never collide in one context.
Uncomment the `jobOperator.stop(...)` lines in `JobRunner` to watch the manager issue a
stop while partitions run on the worker JVM(s) — the **null-lock** branch this review is about.

### Packaging instead of `spring-boot:run`

```bash
./mvnw -DskipTests package
java -jar target/remote-partitioning-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=local
```

> This folder is a standalone project, unrelated to the surrounding spring-batch Maven
> reactor. If your available Spring Boot 4.x differs, adjust the parent version in `pom.xml`.

## Profiles at a glance

| Profile  | Handler | Transport | Store | Runs offline? |
|----------|---------|-----------|-------|---------------|
| `local`  | `TaskExecutorPartitionHandler` (in-JVM threads) | none | H2 (in-memory) | ✅ yes |
| `manager`/`worker` | `MessageChannelPartitionHandler` (remote) | RabbitMQ | Postgres | ❌ needs infra |
