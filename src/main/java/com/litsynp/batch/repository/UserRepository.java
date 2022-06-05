package com.litsynp.batch.repository;

import com.litsynp.batch.domain.User;
import com.litsynp.batch.domain.enums.Grade;
import com.litsynp.batch.domain.enums.UserStatus;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {

    List<User> findByUpdatedDateBeforeAndStatusEquals(LocalDateTime beforeUpdatedDate,
            UserStatus status);

    List<User> findByUpdatedDateBeforeAndStatusEqualsAndGradeEquals(LocalDateTime beforeUpdatedDate,
            UserStatus status, Grade grade);
}
