/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp2.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Component;
import org.springframework.util.PatternMatchUtils;

import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.exception.NoJobToExecuteException;
import uk.ac.ebi.eva.commons.batch.exception.NoParametersHaveBeenPassedException;
import uk.ac.ebi.eva.commons.batch.exception.NoPreviousJobExecutionException;
import uk.ac.ebi.eva.commons.batch.job.JobStatusManager;

import java.util.Collection;

/**
 * Custom job launcher command line runner to integrate Job with Input Parameters
 */
@Component
public class DbsnpJsonImportVariantsJobLauncherCommandLineRunner extends JobLauncherCommandLineRunner implements
        ApplicationEventPublisherAware, ExitCodeGenerator {

    private static final Logger logger = LoggerFactory
            .getLogger(DbsnpJsonImportVariantsJobLauncherCommandLineRunner.class);

    public static final String SPRING_BATCH_JOB_NAME_PROPERTY = "spring.batch.job.names";

    public static final int EXIT_WITHOUT_ERRORS = 0;

    public static final int EXIT_WITH_ERRORS = 1;

    @Value("${" + SPRING_BATCH_JOB_NAME_PROPERTY + ":#{null}}")
    private String jobName;

    private Collection<Job> jobs;

    private JobRepository jobRepository;

    @Autowired
    private InputParameters inputParameters;

    private boolean abnormalExit;

    public DbsnpJsonImportVariantsJobLauncherCommandLineRunner(JobLauncher jobLauncher, JobExplorer jobExplorer,
                                                           JobRepository jobRepository) {
        super(jobLauncher, jobExplorer);
        this.jobRepository = jobRepository;
    }


    @Autowired(required = false)
    public void setJobs(Collection<Job> jobs) {
        this.jobs = jobs;
    }

    @Override
    public void setJobNames(String jobName) {
        this.jobName = jobName;
        super.setJobNames(jobName);
    }

    @Override
    public int getExitCode() {
        if (!abnormalExit) {
            return EXIT_WITHOUT_ERRORS;
        } else {
            return EXIT_WITH_ERRORS;
        }
    }

    @Override
    public void run(String... args) throws JobExecutionException {
        try {
            abnormalExit = false;

            JobParameters jobParameters = inputParameters.toJobParameters();

            JobStatusManager.checkIfJobNameHasBeenDefined(jobName);
            JobStatusManager.checkIfPropertiesHaveBeenProvided(jobParameters);
            if (inputParameters.isForceRestart()) {
                markPreviousJobAsFailed(jobParameters);
            }
            launchJob(jobParameters);
        } catch (NoPreviousJobExecutionException | NoParametersHaveBeenPassedException | NoJobToExecuteException
                | JobParametersInvalidException | JobExecutionAlreadyRunningException e) {
            logger.error(e.getMessage());
            logger.debug("Error trace", e);
            abnormalExit = true;
        }

    }

    private void launchJob(JobParameters jobParameters) throws JobExecutionException {
        for (Job job : this.jobs) {
            if (PatternMatchUtils.simpleMatch(jobName, job.getName())) {
                execute(job, jobParameters);
                return;
            }
        }
    }

    @Override
    protected void execute(Job job, JobParameters jobParameters) throws JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException,
            JobParametersNotFoundException {
        logger.info("Running job '" + jobName + "' with parameters: " + jobParameters);
        super.execute(job, jobParameters);
    }

    private void markPreviousJobAsFailed(JobParameters jobParameters) throws
            NoPreviousJobExecutionException {
        logger.info("Force restartPreviousExecution of job '" + jobName + "' with parameters: " + jobParameters);
        JobStatusManager.markLastJobAsFailed(jobRepository, jobName, jobParameters);
    }
}
