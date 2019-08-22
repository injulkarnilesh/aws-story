package in.injulkar.nilesh.boot.awsstory.services;

import in.injulkar.nilesh.boot.awsstory.persistence.Student;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StudentDao extends JpaRepository<Student, Long> {

}
