package in.injulkar.nilesh.boot.awsstory.sqs;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ImportResource(value = "classpath:aws-story-sqs.xml")
public class SQSConfig {

    public static final String QUEUE_NAME = "aws-story-queue";

//    @Bean
//    public AmazonSQSAsync amazonSQSAsyncClient() {
//        return AmazonSQSAsyncClient.asyncBuilder()
//                .withRegion(Regions.US_EAST_1)
//                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
//                .build();
//    }

    @Bean
    public QueueMessagingTemplate awsQueueMessagingTemplate(final AmazonSQSAsync amazonSQS) {
        return new QueueMessagingTemplate(amazonSQS);
    }
}
