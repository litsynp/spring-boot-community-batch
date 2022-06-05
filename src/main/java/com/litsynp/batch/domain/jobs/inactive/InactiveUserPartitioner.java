package com.litsynp.batch.domain.jobs.inactive;

import com.litsynp.batch.domain.enums.Grade;
import java.util.HashMap;
import java.util.Map;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

/**
 * 회원의 등급에 따라 파티션을 분할한다.
 * <p>
 * 스프링 부트 배치에서는 이미 Partitioner 인터페이스를 구현한 SimplePartitioner와 MultiResourcePartitioner를 제공한다.
 * <p>
 * 이미 구현된 클래스를 사용해도 상관없지만, 등급에 따라 Step을 할당하도록 Partitioner 인터페이스를 구현해본다.
 */
public class InactiveUserPartitioner implements Partitioner {

    private static final String GRADE = "grade";
    private static final String INACTIVE_USER_TASK = "InactiveUserTask";

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        // gridSize 만큼 Map 할당
        Map<String, ExecutionContext> map = new HashMap<>(gridSize);

        // Grade Enum에 정의된 모든 값을 배열에 넣음
        Grade[] grades = Grade.values();

        // grades 값만큼 파티션을 생성하는 루프
        for (int i = 0, length = grades.length; i < length; i++) {
            ExecutionContext context = new ExecutionContext();
            // Context 설정. 파라미터로 Grade 값을 받아 사용한다. 키는 'grade'이다. Grade Enum의 이름값을 context에 추가한다.
            context.putString(GRADE, grades[i].name());
            // 반환되는 map에 InactiveUserTask1..2..3 형식의 파티션 키값을 지정하고, ExecutionContext를 map에 추가한다.
            map.put(INACTIVE_USER_TASK + i, context);
        }
        return map;
    }
}
