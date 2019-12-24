package in.injulkar.nilesh.aws.story.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StudentDynamoDao {

    public static final String STUDENTS = "students";
    public static final String CLASS_STUDENTS = "class-students";
    private final AmazonDynamoDB amazonDynamoDB;

    public StudentDynamoDao(final AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public void createTable(final String tableName, final String keyColumn, final ScalarAttributeType keyType,
                             final String rangeColumn, final ScalarAttributeType rangeType) {
        final List<AttributeDefinition> attributes = new ArrayList<>();
        final AttributeDefinition key = new AttributeDefinition();
        key.setAttributeName(keyColumn);
        key.setAttributeType(keyType);

        final AttributeDefinition range = new AttributeDefinition();
        range.setAttributeName(rangeColumn);
        range.setAttributeType(rangeType);

        attributes.add(key);
        attributes.add(range);

        final List<KeySchemaElement> keySchema = new ArrayList<>();
        final KeySchemaElement idSchema = new KeySchemaElement()
                .withAttributeName(keyColumn)
                .withKeyType(KeyType.HASH);
        final KeySchemaElement rangeSchema = new KeySchemaElement()
                .withAttributeName(rangeColumn)
                .withKeyType(KeyType.RANGE);

        keySchema.add(idSchema);
        keySchema.add(rangeSchema);

        final CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributes)
                .withKeySchema(keySchema)
                .withProvisionedThroughput(
                        new ProvisionedThroughput()
                                .withReadCapacityUnits(5L).withWriteCapacityUnits(5L)
                );

        this.amazonDynamoDB.createTable(request);
    }

    public void createTable(final String tableName, final String keyColumn, final ScalarAttributeType keyType) {
        final List<AttributeDefinition> attributes = new ArrayList<>();
        final AttributeDefinition key = new AttributeDefinition();
        key.setAttributeName(keyColumn);
        key.setAttributeType(keyType);
        attributes.add(key);

        final List<KeySchemaElement> keySchema = new ArrayList<>();
        final KeySchemaElement idSchema = new KeySchemaElement()
                .withAttributeName(keyColumn)
                .withKeyType(KeyType.HASH);
        keySchema.add(idSchema);

        final CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributes)
                .withKeySchema(keySchema)
                .withProvisionedThroughput(
                        new ProvisionedThroughput()
                                .withReadCapacityUnits(5L).withWriteCapacityUnits(5L)
                );

        this.amazonDynamoDB.createTable(request);
    }

    public void saveStudent(final Student student) {
        final Map<String, AttributeValue> item = toPutItemMap(student);
        final PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(STUDENTS)
                .withItem(item);
        amazonDynamoDB.putItem(putItemRequest);
    }

    public List<Student> getAllStudents() {
        final List<String> attributes = new ArrayList<>();
        attributes.add("email");
        attributes.add("name");
        attributes.add("surname");
        final ScanResult scan = amazonDynamoDB.scan(STUDENTS, attributes);
        return scan.getItems().stream()
                .map(this::toStudent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    public Optional<Student> findByEmail(final String email) {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("email", new AttributeValue().withS(email));
        final GetItemRequest request = new GetItemRequest()
                .withTableName(STUDENTS)
                .withAttributesToGet("email", "name", "surname")
                .withKey(key);
        final GetItemResult item = amazonDynamoDB.getItem(request);
        return toStudent(item.getItem());
    }

    private Optional<Student> toStudent(final Map<String, AttributeValue> m) {
        if (m == null || m.size() == 0) {
            return Optional.empty();
        }
        final Student s = new Student();
        s.setEmail(m.get("email").getS());
        s.setName(m.get("name").getS());
        s.setSurname(m.get("surname").getS());
        return Optional.of(s);
    }

    public void saveStudentsInClass(final String className, final Student ...students) {
        for (Student student: students) {
            final Map<String, AttributeValue> item = toClassStudentPutItem(className, student);
            final PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(CLASS_STUDENTS)
                    .withItem(item);
            amazonDynamoDB.putItem(putItemRequest);
        }
    }

    private Map<String, AttributeValue> toClassStudentPutItem(final String className, final Student student) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("class", new AttributeValue().withS(className));
        item.put("email", new AttributeValue().withS(student.getEmail()));
        item.put("name", new AttributeValue().withS(student.getName()));
        item.put("surname", new AttributeValue().withS(student.getSurname()));
        return item;
    }

    public List<Student> getAllStudentsOfClass(final String className) {
        final Map<String, AttributeValue> expression = new HashMap<>();
        expression.put(":class", new AttributeValue().withS(className));

        final Map<String,String> attributeNames = new HashMap<>();
        attributeNames.put("#class","class");

        final QueryRequest queryRequest = new QueryRequest()
                .withTableName(CLASS_STUDENTS)
                .withExpressionAttributeNames(attributeNames)
                .withExpressionAttributeValues(expression)
                .withKeyConditionExpression("#class = :class");

        final QueryResult queryResult = amazonDynamoDB.query(queryRequest);

        final List<Map<String, AttributeValue>> items = queryResult.getItems();

        return items.stream().map(this::toStudent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    public Optional<Student> getStudentOfClass(final String className, final String email) {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("class", new AttributeValue().withS(className));
        key.put("email", new AttributeValue().withS(email));

        final Map<String, KeysAndAttributes> attributes = new HashMap<>();
        attributes.put(CLASS_STUDENTS, new KeysAndAttributes()
                .withKeys(key));
        final BatchGetItemRequest request = new BatchGetItemRequest()
                .withRequestItems(attributes);
        final BatchGetItemResult itemResult = amazonDynamoDB.batchGetItem(request);
        final Map<String, List<Map<String, AttributeValue>>> responses = itemResult.getResponses();
        final List<Map<String, AttributeValue>> students = responses.get(CLASS_STUDENTS);

        return students.stream()
                .map(this::toStudent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void saveStudentsInClassTx(final String className, final Student student) {
        final Map<String, AttributeValue> studentItem = toPutItemMap(student);
        final Map<String, AttributeValue> classStudentItem = toClassStudentPutItem(className, student);
        final Put saveToStudents = new Put()
                .withTableName(STUDENTS)
                .withItem(studentItem);
        final Put saveToClassStudents = new Put()
                .withTableName(CLASS_STUDENTS)
                .withItem(classStudentItem);

        final TransactWriteItem writeToStudents = new TransactWriteItem().withPut(saveToStudents);
        final TransactWriteItem writeToClassStudents = new TransactWriteItem().withPut(saveToClassStudents);
        final TransactWriteItemsRequest txRequest = new TransactWriteItemsRequest()
                .withTransactItems(writeToStudents, writeToClassStudents);

        amazonDynamoDB.transactWriteItems(txRequest);
    }

    public void updateStudent(final Student student) {
        final UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(STUDENTS)
                .addKeyEntry("email", new AttributeValue().withS(student.getEmail()))
                .addAttributeUpdatesEntry("name", new AttributeValueUpdate().withValue(new AttributeValue().withS(student.getName())))
                .addAttributeUpdatesEntry("surname", new AttributeValueUpdate().withValue(new AttributeValue().withS(student.getSurname())));
        amazonDynamoDB.updateItem(updateItemRequest);
    }

    private Map<String, AttributeValue> toPutItemMap(final Student student) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("email", new AttributeValue().withS(student.getEmail()));
        item.put("name", new AttributeValue().withS(student.getName()));
        item.put("surname", new AttributeValue().withS(student.getSurname()));
        return item;
    }
}
