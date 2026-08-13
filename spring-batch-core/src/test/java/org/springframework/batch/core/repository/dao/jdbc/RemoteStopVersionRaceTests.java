/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.core.repository.dao.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.support.incrementer.H2SequenceMaxValueIncrementer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Reproduces the cross-JVM (remote partitioning / remote chunking) race that the
 * per-execution {@code Semaphore} added in GH-5308 cannot cover, because a semaphore does
 * not cross JVMs and {@code AbstractStep.callUnderLock} runs the action UNLOCKED when the
 * step execution is not registered in this JVM's lock map ("null lock branch").
 *
 * <p>
 * Two holders of the SAME {@code BATCH_STEP_EXECUTION} row:
 * <ul>
 * <li>the <b>worker</b> (its own JVM) commits a chunk, bumping VERSION 0 -&gt; 1;</li>
 * <li>the <b>manager</b> (the JobOperator JVM) persists STOPPED while still holding a
 * stale in-memory copy at VERSION 0.</li>
 * </ul>
 *
 * <p>
 * Before this PR, {@code SimpleJobRepository.update(StepExecution)} reconciled the stale
 * version via {@code stepExecutionDao.synchronizeStatus(stepExecution)} inside the
 * stopping branch, which absorbed this race. This PR removed that call, so the manager's
 * stop update now loses the optimistic-locking check. The added {@code AbstractStepTests}
 * case only exercises one {@code execute()} on a single JVM, so it stays green even
 * though this remote path regresses.
 *
 * <p>
 * {@link #managerStopWithoutSynchronizeStatus_losesOptimisticLock()} documents the
 * regression (currently RED on this branch).
 * {@link #managerStopWithSynchronizeStatus_reconcilesAndSucceeds()} shows the reviewer's
 * proposed fix (keep {@code synchronizeStatus} for the null-lock branch) makes it pass.
 */
class RemoteStopVersionRaceTests {

	private JdbcStepExecutionDao stepExecutionDao;

	private JdbcJobExecutionDao jobExecutionDao;

	private JdbcJobInstanceDao jobInstanceDao;

	@BeforeEach
	void setup() throws Exception {
		EmbeddedDatabase database = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
			.addScript("/org/springframework/batch/core/schema-drop-h2.sql")
			.addScript("/org/springframework/batch/core/schema-h2.sql")
			.build();
		JdbcTemplate jdbcTemplate = new JdbcTemplate(database);

		jobInstanceDao = new JdbcJobInstanceDao();
		jobInstanceDao.setJdbcTemplate(jdbcTemplate);
		jobInstanceDao.setJobInstanceIncrementer(new H2SequenceMaxValueIncrementer(database, "BATCH_JOB_INSTANCE_SEQ"));
		jobInstanceDao.afterPropertiesSet();

		jobExecutionDao = new JdbcJobExecutionDao();
		jobExecutionDao.setJdbcTemplate(jdbcTemplate);
		jobExecutionDao
			.setJobExecutionIncrementer(new H2SequenceMaxValueIncrementer(database, "BATCH_JOB_EXECUTION_SEQ"));
		jobExecutionDao.setJobInstanceDao(jobInstanceDao);
		jobExecutionDao.afterPropertiesSet();

		stepExecutionDao = new JdbcStepExecutionDao();
		stepExecutionDao.setJdbcTemplate(jdbcTemplate);
		stepExecutionDao
			.setStepExecutionIncrementer(new H2SequenceMaxValueIncrementer(database, "BATCH_STEP_EXECUTION_SEQ"));
		stepExecutionDao.setJobExecutionDao(jobExecutionDao);
		stepExecutionDao.afterPropertiesSet();
	}

	/**
	 * Regression: with {@code synchronizeStatus} removed from the stopping branch, the
	 * manager's stop update against a concurrently bumped version throws. On a single JVM
	 * the semaphore would have serialised the two writers; across JVMs it cannot, and the
	 * stop takes the null-lock (unlocked) branch, so nothing protects it now.
	 */
	@Test
	void managerStopWithoutSynchronizeStatus_losesOptimisticLock() {
		StepExecution managerView = givenRunningStepExecution();

		// Worker JVM commits a chunk -> DB VERSION 0 -> 1 (manager's copy is now stale).
		workerBumpsVersionInItsOwnJvm(managerView.getId());

		// Manager stop persists STOPPED via the null-lock branch (no synchronizeStatus).
		managerView.setStatus(BatchStatus.STOPPED);
		assertThrows(OptimisticLockingFailureException.class, () -> stepExecutionDao.updateStepExecution(managerView),
				"Expected the stale stop update to fail once synchronizeStatus was removed");
	}

	/**
	 * The reviewer's proposed fix: restore {@code synchronizeStatus} for the null-lock
	 * branch. It re-reads VERSION and resets the stale in-memory copy, so the stop update
	 * matches the current row and succeeds.
	 */
	@Test
	void managerStopWithSynchronizeStatus_reconcilesAndSucceeds() {
		StepExecution managerView = givenRunningStepExecution();

		workerBumpsVersionInItsOwnJvm(managerView.getId());

		// This is exactly the line removed by the PR (kept here only for the remote
		// path).
		stepExecutionDao.synchronizeStatus(managerView);

		managerView.setStatus(BatchStatus.STOPPED);
		assertDoesNotThrow(() -> stepExecutionDao.updateStepExecution(managerView),
				"After reconciliation the stop update should match the current row and succeed");

		StepExecution reloaded = stepExecutionDao.getStepExecution(managerView.getId());
		assertEquals(BatchStatus.STOPPED, reloaded.getStatus());
	}

	private StepExecution givenRunningStepExecution() {
		JobInstance jobInstance = jobInstanceDao.createJobInstance("job", new JobParameters());
		JobExecution jobExecution = jobExecutionDao.createJobExecution(jobInstance, new JobParameters());
		StepExecution stepExecution = stepExecutionDao.createStepExecution("workerStep", jobExecution);
		stepExecution.setStatus(BatchStatus.STARTED);
		stepExecutionDao.updateStepExecution(stepExecution);
		return stepExecution;
	}

	/** Stand-in for the remote worker updating the row from its own JVM. */
	private void workerBumpsVersionInItsOwnJvm(long stepExecutionId) {
		StepExecution workerView = stepExecutionDao.getStepExecution(stepExecutionId);
		workerView.getExecutionContext().putString("chunk", "committed");
		stepExecutionDao.updateStepExecution(workerView);
	}

}
