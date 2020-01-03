package in.injulkar.nilesh.aws.story.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
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
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StudentDynamoDao {

    private static final String STUDENTS = "students";
    private static final String CLASS_STUDENTS = "class-students";
    private static final String INDEX_BY_NAME = "IndexByName";
    private static final String EMAIL = "email";
    private static final String FIRSTNAME = "firstname";
    private static final String SURNAME = "surname";

    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDB dynamoDB;

    public StudentDynamoDao(final AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDB = new DynamoDB(amazonDynamoDB);
    }

    public void createTable(final String tableName, final String keyColumn, final ScalarAttributeType keyType,
                             final String rangeColumn, final ScalarAttributeType rangeType) {
        final AttributeDefinition key = attributeDef(keyColumn, keyType);
        final AttributeDefinition range = attributeDef(rangeColumn, rangeType);
        final List<AttributeDefinition> attributes = Arrays.asList(key, range);

        final KeySchemaElement idSchema = schemaKey(keyColumn, KeyType.HASH);
        final KeySchemaElement rangeSchema = schemaKey(rangeColumn, KeyType.RANGE);

        final List<KeySchemaElement> keySchema = Arrays.asList(idSchema, rangeSchema);

        createTable(tableName, attributes, keySchema);
    }

    public void createTable(final String tableName, final String keyColumn, final ScalarAttributeType keyType) {
        final AttributeDefinition key = attributeDef(keyColumn, keyType);
        final List<AttributeDefinition> attributes = Arrays.asList(key);

        final KeySchemaElement idSchema = schemaKey(keyColumn, KeyType.HASH);
        final List<KeySchemaElement> keySchema = Arrays.asList(idSchema);

        createTable(tableName, attributes, keySchema);
    }

    public void createTableWithLocalSecondaryIndex(final String tableName, final String keyColumn, final ScalarAttributeType keyType,
                                                   final String rangeColumn, final ScalarAttributeType rangeType, final String indexColumn, final ScalarAttributeType indexColumnType) {
        final AttributeDefinition key = attributeDef(keyColumn, keyType);
        final AttributeDefinition range = attributeDef(rangeColumn, rangeType);
        final AttributeDefinition indexAttr = new AttributeDefinition().withAttributeName(indexColumn)
                .withAttributeType(indexColumnType);
        final List<AttributeDefinition> attributes = Arrays.asList(key, range, indexAttr);

        final KeySchemaElement idSchema = schemaKey(keyColumn, KeyType.HASH);
        final KeySchemaElement rangeSchema = schemaKey(rangeColumn, KeyType.RANGE);
        final List<KeySchemaElement> primarySchema = Arrays.asList(idSchema, rangeSchema);

        final Projection projection = new Projection().withProjectionType(ProjectionType.ALL);
        final KeySchemaElement indexRange = schemaKey(indexColumn, KeyType.RANGE);

        final LocalSecondaryIndex localSecondaryIndex = new LocalSecondaryIndex()
                .withIndexName(INDEX_BY_NAME)
                .withKeySchema(Arrays.asList(idSchema, indexRange))
                .withProjection(projection);

        final CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributes)
                .withKeySchema(primarySchema)
                .withLocalSecondaryIndexes(localSecondaryIndex)
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
        attributes.add(EMAIL);
        attributes.add(FIRSTNAME);
        attributes.add(SURNAME);
        final ScanResult scan = amazonDynamoDB.scan(STUDENTS, attributes);
        return scan.getItems().stream()
                .map(this::toStudent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    public Optional<Student> findByEmail(final String email) {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put(EMAIL, new AttributeValue().withS(email));
        final GetItemRequest request = new GetItemRequest()
                .withTableName(STUDENTS)
                .withAttributesToGet(EMAIL, FIRSTNAME, SURNAME)
                .withKey(key);
        final GetItemResult item = amazonDynamoDB.getItem(request);
        return toStudent(item.getItem());
    }

    private Optional<Student> toStudent(final Map<String, AttributeValue> m) {
        if (m == null || m.size() == 0) {
            return Optional.empty();
        }
        final Student s = new Student();
        s.setEmail(m.get(EMAIL).getS());
        s.setName(m.get(FIRSTNAME).getS());
        s.setSurname(m.get(SURNAME).getS());
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
        item.put("classname", new AttributeValue().withS(className));
        item.put(EMAIL, new AttributeValue().withS(student.getEmail()));
        item.put(FIRSTNAME, new AttributeValue().withS(student.getName()));
        item.put(SURNAME, new AttributeValue().withS(student.getSurname()));
        return item;
    }

    public List<Student> getAllStudentsOfClass(final String className) {
        final Map<String, AttributeValue> expression = new HashMap<>();
        expression.put(":classname", new AttributeValue().withS(className));

        final Map<String,String> attributeNames = new HashMap<>();
        attributeNames.put("#classname", "classname");

        final QueryRequest queryRequest = new QueryRequest()
                .withTableName(CLASS_STUDENTS)
                .withExpressionAttributeNames(attributeNames)
                .withExpressionAttributeValues(expression)
                .withKeyConditionExpression("#classname = :classname");

        final QueryResult queryResult = amazonDynamoDB.query(queryRequest);

        final List<Map<String, AttributeValue>> items = queryResult.getItems();

        return items.stream().map(this::toStudent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    public Optional<Student> getStudentOfClass(final String className, final String email) {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("classname", new AttributeValue().withS(className));
        key.put(EMAIL, new AttributeValue().withS(email));
        return findStudentByKeys(key);
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
                .addKeyEntry(EMAIL, new AttributeValue().withS(student.getEmail()))
                .addAttributeUpdatesEntry(FIRSTNAME, new AttributeValueUpdate().withValue(new AttributeValue().withS(student.getName())))
                .addAttributeUpdatesEntry(SURNAME, new AttributeValueUpdate().withValue(new AttributeValue().withS(student.getSurname())));
        amazonDynamoDB.updateItem(updateItemRequest);
    }

    public List<Student> getStudentsOfClassWithName(final String className, final String name) {
        QuerySpec querySpec = new QuerySpec().withConsistentRead(true).withScanIndexForward(true)
            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

        final Table table = dynamoDB.getTable(CLASS_STUDENTS);
        final Index index = table.getIndex(INDEX_BY_NAME);

        querySpec.withKeyConditionExpression("classname = :classname and firstname = :firstname")
                .withValueMap(new ValueMap().withString(":classname", className).withString(":firstname", name));

        querySpec.withProjectionExpression("firstname, surname, email");

        ItemCollection<QueryOutcome> items = index.query(querySpec);
        final List<Student> students = new ArrayList<>();
        for (final Item item : items) {
            final String nm = item.getString(FIRSTNAME);
            final String sn = item.getString(SURNAME);
            final String e = item.getString(EMAIL);
            final Student s = new Student();
            s.setName(nm);
            s.setSurname(sn);
            s.setEmail(e);
            students.add(s);
        }
        return students;
    }

    private Optional<Student> findStudentByKeys(final Map<String, AttributeValue> keys) {
        final Map<String, KeysAndAttributes> attributes = new HashMap<>();
        attributes.put(CLASS_STUDENTS, new KeysAndAttributes().withKeys(keys));

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

    private Map<String, AttributeValue> toPutItemMap(final Student student) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put(EMAIL, new AttributeValue().withS(student.getEmail()));
        item.put(FIRSTNAME, new AttributeValue().withS(student.getName()));
        item.put(SURNAME, new AttributeValue().withS(student.getSurname()));
        return item;
    }

    private void createTable(final String tableName, final List<AttributeDefinition> attributes, final List<KeySchemaElement> keySchema) {
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

    private AttributeDefinition attributeDef(final String keyColumn, final ScalarAttributeType keyType) {
        final AttributeDefinition key = new AttributeDefinition();
        key.setAttributeName(keyColumn);
        key.setAttributeType(keyType);
        return key;
    }

    private KeySchemaElement schemaKey(final String keyColumn, final KeyType type) {
        return new KeySchemaElement()
                .withAttributeName(keyColumn)
                .withKeyType(type);
    }

    public void dropTable(final String tableName) {
        amazonDynamoDB.deleteTable(tableName);
    }
}
