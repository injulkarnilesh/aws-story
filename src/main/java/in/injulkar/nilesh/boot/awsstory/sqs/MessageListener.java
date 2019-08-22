package in.injulkar.nilesh.boot.awsstory.sqs;


import in.injulkar.nilesh.boot.awsstory.controllers.models.Student;
import in.injulkar.nilesh.boot.awsstory.services.StudentDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {

    private StudentDao studentDao;

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageListener.class);

    @Autowired
    public MessageListener(final StudentDao studentDao) {
        this.studentDao = studentDao;
    }

    @SqsListener(SQSConfig.QUEUE_NAME)
    public void onMessage(final Student student) {
        LOGGER.info("Received message {}", student);
        final in.injulkar.nilesh.boot.awsstory.persistence.Student entity =
                new in.injulkar.nilesh.boot.awsstory.persistence.Student();
        entity.setFirstName(student.getFirstName());
        entity.setLastName(student.getLastName());
        studentDao.saveAndFlush(entity);
    }
}
