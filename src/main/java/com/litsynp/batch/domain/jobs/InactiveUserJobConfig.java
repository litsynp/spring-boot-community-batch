package com.litsynp.batch.domain.jobs;

import com.litsynp.batch.domain.User;
import com.litsynp.batch.domain.enums.UserStatus;
import com.litsynp.batch.domain.jobs.readers.QueueItemReader;
import com.litsynp.batch.repository.UserRepository;
import java.time.LocalDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
public class InactiveUserJobConfig {

    private UserRepository userRepository;

    @Bean
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory, Step inactiveJobStep) {
        return jobBuilderFactory.get("inactiveUserJob")
                .preventRestart()  // Job의 재실행을 막음
                .start(inactiveJobStep)
                .build();
    }

    @Bean
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory) {
        // Input & Output 타입을 User로 설정하고, 쓰기 시에 청크 단위로 묶어서 writer() 메서드를 실행시킬 단위인 chunk 설정
        // 즉, 커밋의 단위가 10개
        return stepBuilderFactory.get("inactiveUserStep")
                .<User, User>chunk(10)
                .reader(inactiveUserReader())
                .processor(inactiveUserProcessor())
                .writer(inactiveUserWriter())
                .build();
    }

    @Bean
    @StepScope
    public QueueItemReader<User> inactiveUserReader() {
        // 기본 빈 생성은 싱글턴이지만, @StepScope를 사용하면 해당 메서드는 Step의 주기에 따라 새로운 빈을 생성한다.
        // 각 Step의 실행마다 새로 빈을 만들기 때문에 지연 생성이 가능하다.
        // 주의할 점은, @StepScope는 기본 프록시 모드가 반환되는 클래스 타입을 참조하기 때문에 @StepScope를 사용하면 반드시 구현된 반환 타입을 명시해 사용해야 한다.
        // 여기서는 QueueItemReader<User>라고 명시했다.
        List<User> oldUsers = userRepository.findByUpdatedDateBeforeAndStatusEquals(
                LocalDateTime.now().minusYears(1),
                UserStatus.ACTIVE);
        return new QueueItemReader<>(oldUsers);
    }

    public ItemProcessor<User, User> inactiveUserProcessor() {
        // Simpler Way from Java 8 with "Method Reference"
        // return User::setInactive;
        return new ItemProcessor<User, User>() {
            @Override
            public User process(User user) throws Exception {
                return user.setInactive();
            }
        };
    }

    public ItemWriter<User> inactiveUserWriter() {
        // Replacing ItemWriter write() method overriding with lambda function
        return ((List<? extends User> users) -> userRepository.saveAll(users));
    }
}
