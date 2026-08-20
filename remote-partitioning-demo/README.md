# Remote Partitioning Demo — and why PR #5448's null-lock branch matters

Two runnable pieces:

1. **This demo** (`remote-partitioning-demo/`) — a plain-Java (no Spring Boot) project that
   depends on the locally built spring-batch 6.0.5-SNAPSHOT modules. It has a `local`
   profile (in-JVM range partitioning, runs against MySQL) and `manager`/`worker` profiles
   (a real cross-JVM split over RabbitMQ). See **Running** below.
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

## Running (no Spring Boot)

This is a plain-Java app wired like `spring-batch-samples`: its `pom.xml` uses the
spring-batch **reactor** as its `<parent>` and depends on the locally built
`spring-batch-core` / `spring-batch-integration` **6.0.5-SNAPSHOT** — not jars pinned by a
Spring Boot BOM. Batch infrastructure is hand-wired (`@EnableBatchProcessing` +
`@EnableJdbcJobRepository`, a `DataSource`/`transactionManager`, a `DataSourceInitializer`
for schema, manual RabbitMQ beans). Entry point is `Main`; run it with `exec:java` and a
profile argument. Uses the repo's bundled `./mvnw`.

> Metadata store is MySQL (`jdbc:mysql://localhost:3306/spring_batch`, root / no password).
> Override with `-Ddb.url=… -Ddb.user=… -Ddb.pass=…`.

### A) LOCAL partitioning — no broker (✅ verified end-to-end against MySQL)

Single JVM; partitions run on a thread pool. The `DataSourceInitializer` drops + recreates
the `BATCH_*` and `PEOPLE` tables and seeds 10 rows each run.

```bash
cd remote-partitioning-demo
./mvnw exec:java -Dexec.args=local
```

Expected output — each worker reads only its own ID range:

```
[SimpleAsyncTaskExecutor-1] processed range: [Person[id=1..], ..id=4]
[SimpleAsyncTaskExecutor-2] processed range: [Person[id=5..], ..id=8]
[SimpleAsyncTaskExecutor-3] processed range: [Person[id=9..], id=10]
Job [localPartitionedJob] finished with status COMPLETED
```
```
BATCH_STEP_EXECUTION: managerStep(READ 10) + workerStep:partition0/1/2 (READ 4/4/2)
```

Every worker StepExecution runs in THIS JVM, so a stop here takes the semaphore **locked**
branch — the case PR #5448 fixes directly.

### B) REMOTE partitioning — real manager/worker split (needs RabbitMQ + shared MySQL)

```bash
cd remote-partitioning-demo
docker compose up -d          # rabbitmq (management UI on :15672, guest/guest)
# shared MySQL: reuse your local spring_batch DB (manager prepares the schema)

# terminal 1..n — worker(s): listen and execute partitions
./mvnw exec:java -Dexec.args=worker
# terminal — manager: splits + sends StepExecutionRequests, then waits
./mvnw exec:java -Dexec.args=manager
```

`@Profile("manager")` / `@Profile("worker")` keep each process to its own half. The worker
profile does NOT initialize the schema (it connects to the DB the manager prepared). The
worker's `StepExecutionRequestHandler` is where `workerStep.execute()` runs — in a
DIFFERENT JVM from the manager's `JobOperator`, so a stop there takes the **null-lock**
branch this review is about. (Not verified offline — needs a running RabbitMQ.)

## Profiles at a glance

| Profile  | Handler | Transport | Schema init | Verified here |
|----------|---------|-----------|-------------|---------------|
| `local`  | `TaskExecutorPartitionHandler` (in-JVM threads) | none | drop+create BATCH_* & PEOPLE | ✅ against MySQL |
| `manager` | `MessageChannelPartitionHandler` (remote) | RabbitMQ | drop+create BATCH_* | compiles; needs RabbitMQ |
| `worker` | runs `workerStep.execute()` per request | RabbitMQ | none (uses manager's) | compiles; needs RabbitMQ |
