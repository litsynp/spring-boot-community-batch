package com.litsynp.batch.domain.jobs.inactive;

import com.litsynp.batch.domain.User;
import com.litsynp.batch.domain.enums.UserStatus;
import com.litsynp.batch.domain.jobs.inactive.listener.InactiveJobListener;
import com.litsynp.batch.domain.jobs.inactive.listener.InactiveStepListener;
import com.litsynp.batch.repository.UserRepository;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
public class InactiveUserJobConfig {

    private static final int CHUNK_SIZE = 15;
    private final EntityManagerFactory entityManagerFactory;
    private UserRepository userRepository;

    @Bean
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory,
            InactiveJobListener inactiveJobListener, Step inactiveJobStep) {
        return jobBuilderFactory.get("inactiveUserJob")
                .preventRestart()  // Job의 재실행을 막음
                .listener(inactiveJobListener)  // Job Listener 등록
                .start(inactiveJobStep)
                .build();
    }

    @Bean
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory,
            InactiveStepListener inactiveStepListener) {
        // Input & Output 타입을 User로 설정하고, 쓰기 시에 청크 단위로 묶어서 writer() 메서드를 실행시킬 단위인 chunk 설정
        // 즉, 커밋의 단위가 10개
        return stepBuilderFactory.get("inactiveUserStep")
                .<User, User>chunk(CHUNK_SIZE)
                .listener(inactiveStepListener)
                .reader(inactiveUserJpaReader())
                .processor(inactiveUserProcessor())
                .writer(inactiveUserWriter())
                .build();
    }

    @Bean
    @StepScope
    public ListItemReader<User> inactiveUserReader(
            // SpEL을 사용해 JobParameters에서 nowDate 파라미터를 전달
            @Value("#{jobParameters[nowDate]}") Date nowDate, UserRepository userRepository) {
        // Date -> LocalDateTime
        LocalDateTime now = LocalDateTime.ofInstant(nowDate.toInstant(), ZoneId.systemDefault());

        // 기본 빈 생성은 싱글턴이지만, @StepScope를 사용하면 해당 메서드는 Step의 주기에 따라 새로운 빈을 생성한다.
        // 각 Step의 실행마다 새로 빈을 만들기 때문에 지연 생성이 가능하다.
        // 주의할 점은, @StepScope는 기본 프록시 모드가 반환되는 클래스 타입을 참조하기 때문에 @StepScope를 사용하면 반드시 구현된 반환 타입을 명시해 사용해야 한다.
        // 여기서는 QueueItemReader<User>라고 명시했다.
        List<User> oldUsers = userRepository.findByUpdatedDateBeforeAndStatusEquals(
                LocalDateTime.now().minusYears(1),
                UserStatus.ACTIVE);

        // ListItemReader은 DB에서 한꺼번에 읽어서 메모리에 올리므로 수십 만개 이상의 데이터에서 문제 발생 소지가 있다.
        return new ListItemReader<>(oldUsers);
    }

    /**
     * CHUNK_SIZE에 대해서 주의사항이 있다.
     * <p>
     * JpaPagingItemReader는 내부에 entityManager를 할당받아 사용하는데, 지정한 크기로 데이터를 읽어온다. 만약 inactiveJobStep()
     * 에서 설정한 청크 단위("커밋 단위")가 5라고 가정하면, Item 5개를 writer까지 배치 처리를 진행하고 저장한다고 가정해보자. "저장한 데이터를 바탕으로"
     * 다음에 다시 지정한 크기로 새 인덱스를 할당해 읽어 와야 하는데, 이전에 진행한 5라는 인덱스 값을 그대로 사용해 데이터를 불러오도록 로직이 짜여 있어서 문제가
     * 된다.
     * <p>
     * 예를 들어 청크 단위로 Item 5개를 커밋하고 다음 청크 단위로 넘어 가는 경우를 가정해보자. 하지만 entityManager에서 앞서 처리된 Item 5개 때문에
     * 새로 불러올 Item의 인덱스 시작점이 5로 설정되어 있게 된다. 그러면 쿼리 요청 시 offset 5 (인덱스값), limit 5(지정한 크기 단위)이므로, 개념상
     * 바로 다음 청크 단위 (Item 5개)인 Item을 건너뛰는 상황이 발생한다. 가장 간단한 해결 방법은 조회용 인덱스 값을 항상 0으로 반환하는 것이다.
     */
    @Bean(destroyMethod = "") // To ignore warning message (... destory method 'close' failed ...)
    @StepScope
    public JpaPagingItemReader<User> inactiveUserJpaReader() {
        JpaPagingItemReader<User> jpaPagingItemReader = new JpaPagingItemReader<>() {
            // 조회용 인덱스 값을 항상 0으로 반환한다.
            // Item 5개를 수정하고 다음 5개를 건너뛰지 않고 원하는 순서/청크 단위로 처리가 가능하다.
            @Override
            public int getPage() {
                return 0;
            }
        };

        // JPQL 생성
        String jpqlQuery = "select u" +
                " from User as u" +
                " where u.updatedDate < :updatedDate and u.status = :status";
        jpaPagingItemReader.setQueryString(jpqlQuery);

        // JPQL 파라미터 설정
        Map<String, Object> map = new HashMap<>();
        LocalDateTime now = LocalDateTime.now();
        map.put("updatedDate", now.minusYears(1));
        map.put("status", UserStatus.ACTIVE);
        jpaPagingItemReader.setParameterValues(map);

        // 트랜잭션을 관리해줄 EntityManagerFactory 설정 및 Pagination을 CHUNK_SIZE로 설정
        jpaPagingItemReader.setEntityManagerFactory(entityManagerFactory);
        jpaPagingItemReader.setPageSize(CHUNK_SIZE);

        return jpaPagingItemReader;
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

//    public ItemWriter<User> inactiveUserWriter() {
//        // Replacing ItemWriter write() method overriding with lambda function
//        return ((List<? extends User> users) -> userRepository.saveAll(users));
//    }

    private JpaItemWriter<User> inactiveUserWriter() {
        JpaItemWriter<User> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }
}
