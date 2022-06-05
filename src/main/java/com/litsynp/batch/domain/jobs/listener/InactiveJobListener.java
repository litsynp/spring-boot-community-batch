package com.litsynp.batch.domain.jobs.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InactiveJobListener implements JobExecutionListener {

    // Interface 방식 Listener 구현
    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Before job");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("After job");
    }
}
