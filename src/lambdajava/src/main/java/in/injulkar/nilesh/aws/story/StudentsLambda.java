package in.injulkar.nilesh.aws.story;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StudentsLambda {


    public static final String STUDENTS_TABLE = "students";

    public Student findByEmail(Student s, Context context) {
        final AmazonDynamoDB dynamoDB = amazonDynamoDB();
        final Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put("email", new AttributeValue().withS(s.getEmail()));
        final GetItemResult itemResult = dynamoDB.getItem(STUDENTS_TABLE, attributes);
        final Map<String, AttributeValue> item = itemResult.getItem();
        final Student student = new Student();
        student.setEmail(item.get("email").getS());
        student.setName(item.get("name").getS());
        student.setSurname(item.get("surname").getS());
        return student;
    }

    public List<Student> getAllCustomers(Object input, Context context) {
        final AmazonDynamoDB dynamoDB = amazonDynamoDB();
        final List<String> attributes = new ArrayList<>();
        attributes.add("email");
        attributes.add("name");
        attributes.add("surname");
        final ScanResult scan = dynamoDB.scan(STUDENTS_TABLE, attributes);
        return scan.getItems().stream()
                .map(m -> {
                    final Student s = new Student();
                    s.setEmail(m.get("email").getS());
                    s.setName(m.get("name").getS());
                    s.setSurname(m.get("surname").getS());
                    return s;
                }).collect(Collectors.toList());
    }

    public String saveStudent(Student student, Context context) {
        final Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put("email", new AttributeValue().withS(student.getEmail()));
        attributes.put("name", new AttributeValue().withS(student.getName()));
        attributes.put("surname", new AttributeValue().withS(student.getSurname()));
        final AmazonDynamoDB dynamoDB = amazonDynamoDB();
        final PutItemRequest putItemRequest = new PutItemRequest(STUDENTS_TABLE, attributes);
        dynamoDB.putItem(putItemRequest);
        return student.getEmail();
    }

    private AmazonDynamoDB amazonDynamoDB() {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

}
