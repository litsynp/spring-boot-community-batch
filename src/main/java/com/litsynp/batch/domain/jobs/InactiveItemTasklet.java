package com.litsynp.batch.domain.jobs;

import com.litsynp.batch.domain.User;
import com.litsynp.batch.domain.enums.UserStatus;
import com.litsynp.batch.repository.UserRepository;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

/**
 * <h2>청크 지향 프로세싱 (chunk oriented processing)</h2>
 * 청크 지향 프로세싱 (chunk oriented processing)이란 트랜잭션 경계 내에서 청크 단위로 데이터를 읽고 생성하는 프로그래밍 기법이다.
 * <p>
 * 청크란 아이템이 트랜잭션에서 커밋되는 수를 뜻한다.
 * <p>
 * read한 데이터 수가 지정한 청크 단위(CHUNK_SIZE)와 일치하면, write를 수행하고 트랜잭션을 커밋한다.
 *
 * <h2>청크 지향 프로세싱의 이점은?</h2>
 * 청크로 나누지 않았을 때는 1000개 중 하나만 실패해도 나머지 999개의 데이터가 롤백된다.
 * <p>
 * 그런데 청크 단위를 10으로 해서 배치 처리를 하면 도중에 배치 처리에 실패하더라도 다른 청크는 영향을 받지 않는다.
 * <p>
 * 스프링 배치에서는 청크 단위의 프로그래밍을 지향한다.
 * <p>
 *
 * <h2>읽기, 처리, 쓰기가 통일된 방식, Tasklet</h2>
 * 청크 지향 프로세싱이 아닌 방식은 Tasklet을 이용하는 방식이다.
 * <p>
 * Tasklet은 임의의 Step을 실행할 때 하나의 작업으로 처리하는 방식이다.
 * <p>
 * 읽기, 처리, 쓰기로 나뉜 방식이 청크 지향 프로세싱이라면, 이를 단일 개념으로 만든 것이 Tasklet이라고 할 수 있다.
 * <p>
 * Tasklet 인터페이스는 내부에 execute() 메서드 하나만 지원한다. 내부에 원하는 단일 작업을 구현하고 작업이 끝나면 RepeatStatus.FINISHED를
 * 반환한다. 작업이 계속된다면 RepeatStatus.CONTINUABLE을 반환한다.
 */
@Component
@AllArgsConstructor
public class InactiveItemTasklet implements Tasklet {

    private UserRepository userRepository;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
            throws Exception {
        // reader
        Date nowDate = (Date) chunkContext.getStepContext().getJobParameters().get("nowDate");
        LocalDateTime now = LocalDateTime.ofInstant(nowDate.toInstant(), ZoneId.systemDefault());
        List<User> inactiveUsers = userRepository.findByUpdatedDateBeforeAndStatusEquals(
                now.minusYears(1), UserStatus.INACTIVE);

        // processor
        inactiveUsers = inactiveUsers.stream()
                .map(User::setInactive)
                .toList();

        // writer
        userRepository.saveAll(inactiveUsers);

        return RepeatStatus.FINISHED;
    }
}
