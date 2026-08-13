# PR #5448 리뷰 대응 — 세션 정리 (핸드오프)

> spring-batch PR **#5448** "GH-5308: Serialize step execution updates with concurrent stop requests"
> 리뷰(nikhiln64, CHANGES_REQUESTED) 대응을 위한 조사/구현 내용.
> 실제 PR 브랜치 = `fix/gh-5308-serialize-step-execution-callback` (HEAD `7526992c4`).
> **이번 산출물(데모·테스트·이 노트)은 브랜치 `analyze-partitioning`에 커밋되어 내 포크(`origin` = github.com/kyungrae/spring-batch)에 push됨.**

---

## 0. 다음 세션에서 바로 할 것

- 산출물은 모두 `analyze-partitioning` 브랜치에 **커밋됨** (집에서: `git fetch origin && git checkout analyze-partitioning`):
  - `remote-partitioning-demo/` — 독립 Spring Boot 프로젝트. **local 파티셔닝(broker 없이 실행됨) + remote 파티셔닝** 둘 다.
  - `spring-batch-core/.../repository/dao/jdbc/RemoteStopVersionRaceTests.java` — **실행 검증된** race 재현 테스트
- 데모 실행(가장 쉬움, 인프라 불필요): `cd remote-partitioning-demo && ./mvnw spring-boot:run -Dspring-boot.run.profiles=local`
- 테스트 재실행: `./mvnw -o -pl spring-batch-core test -Dtest=RemoteStopVersionRaceTests`
- **열린 결정 3가지**는 맨 아래 §7 참고.

---

## 1. PR이 하는 일 (요약)

`JobOperator.stop(jobExecution)`이 스텝 실행 중 호출되면, stop 스레드의 step-execution update와
worker 스레드의 chunk-commit update가 **같은 `BATCH_STEP_EXECUTION` row**에 동시 optimistic-locking
update를 날려 한쪽이 0 row 매칭 → `OptimisticLockingFailureException`. MySQL(REPEATABLE READ)에서 빈번.

**해결책:** 실행 중인 step execution마다 `Semaphore` 하나를 두고(`AbstractStep.stepExecutionLocks`),
worker의 update와 operator의 stop update를 `StoppableStep.callUnderLock(...)`으로 **직렬화**.
추가로 `SimpleJobRepository.update(StepExecution)`의 stopping 분기에서
`stepExecutionDao.synchronizeStatus(stepExecution)` **호출을 제거**(락이 있으니 in-memory 버전이 최신이라는 전제).
그리고 stop 시 상태를 `STOPPING` 대신 바로 `STOPPED`로 세팅.

---

## 2. 리뷰 핵심 (내 최초 이해 → 교정)

최초 이해: ①version 동기화 롤백 ②multi-process 동기화 부재 — **두 개 별건**으로 이해.
**교정:** ①②는 사실 **하나의 인과로 묶인 핵심 우려** + 놓친 **작은 지적 2개**.

### 핵심 우려 (1개): `synchronizeStatus` 제거가 remote(멀티-JVM)에서 회귀
- 제거된 코드: `SimpleJobRepository.update`의 `if (jobExecution.isStopped() || isStopping())` 안
  `this.stepExecutionDao.synchronizeStatus(stepExecution);` 한 줄.
  (diff 확인: merge-base `91a81b6`엔 있었고 PR HEAD `7526992c4`에서 삭제됨.)
- 이 호출은 DB의 VERSION을 다시 읽어 **stale한 in-memory 버전을 DB 값으로 재설정** → 두 주체가
  같은 StepExecution을 쓸 때 OLFE를 막던 장치.
- 새 세마포어 락(`callUnderLock`)은 **같은 JVM writer만** 보호. `getStepExecutionLock`이 null이면
  ("Not executing in this JVM") **락 없이 실행**.
- **remote partitioning/chunking**에서는 worker가 자기 JVM에서 update, manager의 operator는
  **null-lock 경로**로 stop → 예전엔 `synchronizeStatus`가 흡수하던 race가 이제 무방비.
- 리뷰어 요청: **null-lock 분기에서만 `synchronizeStatus` 복원** + **맵에 없는 step을 stop하는
  회귀 테스트 추가**(remote worker 대역).

### 작은 지적 2개 (내가 처음에 놓침)
1. **맵 등록 race window**: `execute()` 초입 `stepExecutionLocks.put`(L222) 이전 / finally `remove`(L370)
   이후엔 맵이 비어 `callUnderLock`이 null-unlocked 분기. 그 창에 stop이 들어오면 직렬화가 조용히 스킵.
   (앞쪽 창=put 이전이 실질 위험; 뒤쪽 창은 worker가 대개 끝난 뒤라 약함.)
2. **STOPPING 상태 소실**: `STOPPING` 건너뛰고 바로 `STOPPED` → 중간 상태 폴링/구분 restart 로직엔
   `STOPPED`만 보임. 의도면 description에 명시 요청.

---

## 3. 관련 코드 좌표 (PR 브랜치 기준)

- `spring-batch-core/.../step/AbstractStep.java`
  - L85 `stepExecutionLocks = new ConcurrentHashMap<>()`
  - L222 `stepExecutionLocks.put(stepExecution.getId(), createSemaphore())` (execute 초입)
  - L370 `stepExecutionLocks.remove(stepExecution.getId())` (finally 마지막)
  - L387 `getStepExecutionLock` → `map.get(id)` (null 가능)
  - L392 `callUnderLock`: `if (semaphore == null) { action.run(); return; }` ← **null-unlocked 분기**
- `spring-batch-core/.../repository/support/SimpleJobRepository.java` L157 `update(StepExecution)`
  — 여기서 `stepExecutionDao.synchronizeStatus(stepExecution)` 제거됨.
- `spring-batch-core/.../repository/dao/jdbc/JdbcStepExecutionDao.java` L316 `synchronizeStatus`
  — VERSION 재조회 후 `stepExecution.setVersion(currentVersion)`.

---

## 4. Remote partitioning 동작원리 (왜 null-lock이 이 그림인가)

- **Manager JVM**: Job 실행. PartitionStep이 일을 N개 StepExecution으로 split, 파티션마다
  `StepExecutionRequest{stepExecutionId, stepName}` 메시지 전송. **JobOperator.stop()이 여기 있음.**
- **Worker JVM(들)**: `StepExecutionRequestHandler.handle()`가 요청 수신 →
  `jobRepository.getStepExecution(id)`로 **공유 DB에서 로드** → `step.execute(stepExecution)` 호출.
  **즉 execute() + 락맵이 worker JVM에 존재.**
- **공유 DB**: 양쪽이 같은 `BATCH_*`를 read/write.

→ manager는 그 파티션의 `execute()`를 돌린 적이 없어 **manager 맵엔 엔트리 없음** →
`getStepExecutionLock`=null → stop update가 락 없이 실행. 세마포어는 JVM 경계를 못 넘으므로
크로스-JVM 직렬화 불가. 이 race를 흡수하던 게 `synchronizeStatus`였음.

핵심 코드 (`StepExecutionRequestHandler`, worker JVM에서 실행):
```java
StepExecution stepExecution = jobRepository.getStepExecution(stepExecutionId); // 공유 DB에서 로드
Step step = stepLocator.getStep(stepName);
step.execute(stepExecution);   // execute()와 락맵이 worker JVM에 존재
```

---

## 5. 생성한 산출물

### (A) 파티셔닝 데모 — `remote-partitioning-demo/` (독립 Spring Boot 프로젝트, Boot 4.0.0 + Batch 6, ✅ 컴파일·실행 검증됨)
`DemoApplication` 하나 + 3개 프로파일. Maven wrapper 번들되어 `./mvnw`로 바로 실행. Boot 4.0.x는 최초 빌드만 인터넷 필요.

| Profile | Handler | Transport | Store | 오프라인 |
|---|---|---|---|---|
| `local` | `TaskExecutorPartitionHandler` (in-JVM 스레드) | 없음 | H2 in-memory | ✅ |
| `manager`/`worker` | `MessageChannelPartitionHandler` | RabbitMQ | Postgres | ❌ (docker compose 필요) |

- **local (권장, 인프라 불필요)**: `cd remote-partitioning-demo && ./mvnw spring-boot:run -Dspring-boot.run.profiles=local`
  → `partition0/1/2`가 `SimpleAsyncTaskExecutor` 스레드에서 동시 실행 후 `localPartitionedJob ... COMPLETED`, exit 0 (실행 확인됨).
  - 파일: `LocalPartitioningConfiguration`(`StepBuilder.partitioner(...).step(worker).gridSize(3).taskExecutor(...)`),
    `LocalJobRunner`, `application-local.yml`(H2).
  - **local = 모든 worker StepExecution이 같은 JVM 맵에 등록 → stop이 세마포어 locked 분기(PR이 고치는 케이스).**
- **remote (실제 멀티-JVM)**: `docker compose up -d` → `...profiles=worker`(여러 개 가능) / `...profiles=manager`.
  - 파일: `ManagerConfiguration`, `WorkerConfiguration`, `JobRunner`(stop 주석 포함), `application.yml`, `docker-compose.yml`.
  - **remote = worker가 다른 JVM → manager stop이 null-lock 분기(리뷰 우려 케이스).** `JobRunner`의 `jobOperator.stop(...)` 주석 풀면 재현.
- **함정(해결됨)**: Boot 4는 autoconfig가 기술별 모듈로 쪼개져서 `spring-boot-starter-batch`만으론 `PlatformTransactionManager`가 안 올라옴 → `spring-boot-starter-jdbc` 명시 추가함.

### (B) race 재현 테스트 — `RemoteStopVersionRaceTests.java` (✅ 실행 검증됨)
- 위치: `spring-batch-core/src/test/java/org/springframework/batch/core/repository/dao/jdbc/`
- DAO 레벨(H2)에서 **결정론적**(스레드 없이) 크로스-JVM race 재현. "맵에 없음 = 직렬화할 세마포어 없음"
  이므로 AbstractStep 안 거치고 plain update 2회로 환원.
- 두 케이스:
  - `managerStopWithoutSynchronizeStatus_losesOptimisticLock` → worker가 VERSION bump 후
    manager stop이 stale update → **`OptimisticLockingFailureException` 던짐**을 assert
    = **현재 PR 브랜치의 회귀 실증**.
  - `managerStopWithSynchronizeStatus_reconcilesAndSucceeds` → `synchronizeStatus` 먼저 호출 →
    성공 = **리뷰어 제안 fix 검증**.
- 실행 결과: `Tests run: 2, Failures: 0, Errors: 0 — BUILD SUCCESS` (offline `./mvnw -o`로 확인).
- 포맷 게이트 있음: 새 파일은 `./mvnw -o -pl spring-batch-core spring-javaformat:apply` 필요할 수 있음.

---

## 6. 확인된 사실 / 함정

- **이번 산출물은 `analyze-partitioning` 브랜치에 커밋되어 내 포크(origin)에 push됨.** 실제 PR 코드(락 제거 등)는
  `fix/gh-5308-serialize-step-execution-callback`(7526992c4)에 있음 — 회귀 테스트가 "회귀"를 증명하려면 PR 코드가 있는
  브랜치에서 돌려야 함(§7-1 참고).
- 이 레포는 **Maven** (`pom.xml`, `mvnw`), Spring Batch **6.0.5-SNAPSHOT**, Java **17**, Spring Integration **7.0.5**.
- `mvn` 바이너리는 없고 `./mvnw`(maven 3.9.9 캐시됨)로 batch-core 테스트는 **오프라인 빌드 가능**.
- 데모(`remote-partitioning-demo/`)는 Spring Boot 4.0.x를 최초 1회 다운로드해야 함 → **첫 빌드는 네트워크 필요**, 이후 캐시됨.
- `existing OptimisticLockingFailureTests.java`는 `@Disabled` ("passes in IDE not CLI").

---

## 7. 열린 결정 (다음 세션에서 정할 것)

1. **`RemoteStopVersionRaceTests`를 PR에 커밋할지** — 리뷰어가 요청한 "remote worker 대역 테스트"에
   정확히 해당. 넣는다면 위치/네이밍 확정.
2. **테스트 레벨**: 지금은 DAO 레벨(결정론적). 리뷰어는 문자 그대로 **`AbstractStep.callUnderLock`을
   관통하는**(맵에 없는 execution으로 null 분기 타는) `AbstractStepTests` 케이스를 원했을 수 있음.
   → callUnderLock 관통 + concurrent bump + "stop 성공" assert(현재 RED) 형태로 바꿀지.
3. **실제 fix 구현**: `SimpleJobRepository.update`의 null-lock(=stopping) 분기에
   `synchronizeStatus` **조건부 복원** (같은 JVM 락은 유지, 크로스-JVM만 재조정). + 맵 race window,
   STOPPING 상태 소실 2개는 코드 수정 or description 보강.

---

## 8. 참고 링크/명령

- PR: https://github.com/spring-projects/spring-batch/pull/5448
- 리뷰 원문 확인: `gh pr view 5448 --repo spring-projects/spring-batch --json reviews`
- 테스트 실행: `./mvnw -o -pl spring-batch-core test -Dtest=RemoteStopVersionRaceTests`
- 포맷 적용: `./mvnw -o -pl spring-batch-core spring-javaformat:apply`
- 제거된 라인 diff: `git diff 91a81b6 7526992c4 -- spring-batch-core/src/main/java/org/springframework/batch/core/repository/support/SimpleJobRepository.java`
