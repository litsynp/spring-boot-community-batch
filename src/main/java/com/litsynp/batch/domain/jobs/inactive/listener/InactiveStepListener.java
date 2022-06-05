package com.litsynp.batch.domain.jobs.inactive.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InactiveStepListener {

    // Annotation 방식 Listener 구현
    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        log.info("Before step");
    }

    @AfterStep
    public void afterStep(StepExecution stepExecution) {
        log.info("After step");
    }
}
