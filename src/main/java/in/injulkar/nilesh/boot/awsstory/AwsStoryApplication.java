package in.injulkar.nilesh.boot.awsstory;

import in.injulkar.nilesh.boot.awsstory.sqs.SQSConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(SQSConfig.class)
public class AwsStoryApplication {

    public static void main(String[] args) {
        SpringApplication.run(AwsStoryApplication.class, args);
    }

}
