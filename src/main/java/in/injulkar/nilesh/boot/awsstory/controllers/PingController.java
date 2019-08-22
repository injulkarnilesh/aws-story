package in.injulkar.nilesh.boot.awsstory.controllers;

import in.injulkar.nilesh.boot.awsstory.controllers.models.Student;
import in.injulkar.nilesh.boot.awsstory.services.StudentDao;
import in.injulkar.nilesh.boot.awsstory.sqs.SQSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;


@RestController
public class PingController {

    @Value("${node}")
    private String node;

    private StudentDao studentDao;
    private QueueMessagingTemplate queueMessagingTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(PingController.class);

    @Autowired
    public PingController(final StudentDao studentDao,
                          final QueueMessagingTemplate queueMessagingTemplate) {
        this.studentDao = studentDao;
        this.queueMessagingTemplate = queueMessagingTemplate;
    }

    @GetMapping("/ping")
    public String ping() {
        return "pong from node " + node;
    }

    @GetMapping("/ping/{message}")
    public String pingWithMessage(@PathVariable() final String message) {
        return new StringBuilder(message).reverse().toString();
    }

    @GetMapping("/students")
    public List<Student> getStudents() {
        final List<in.injulkar.nilesh.boot.awsstory.persistence.Student> students = studentDao.findAll();
        return students.stream().map(s -> {
            final Student student = new Student();
            student.setId(s.getId());
            student.setFirstName(s.getFirstName());
            student.setLastName(s.getLastName());
            return student;
        }).collect(Collectors.toList());
    }

    @PostMapping("/students")
    public Long createStudent(@RequestBody Student student) {
        final in.injulkar.nilesh.boot.awsstory.persistence.Student entity =
                new in.injulkar.nilesh.boot.awsstory.persistence.Student();
        entity.setFirstName(student.getFirstName());
        entity.setLastName(student.getLastName());
        studentDao.saveAndFlush(entity);
        return entity.getId();
    }

    @PostMapping("/sqs/students")
    public void saveStudent(@RequestBody Student student) {
        LOGGER.info("Sending message for {}", student);
        queueMessagingTemplate.convertAndSend(SQSConfig.QUEUE_NAME, student);
        LOGGER.info("Sent message for {}", student);
    }

}
