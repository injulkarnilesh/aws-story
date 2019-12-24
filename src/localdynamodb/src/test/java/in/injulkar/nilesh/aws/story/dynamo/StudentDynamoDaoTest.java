package in.injulkar.nilesh.aws.story.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StudentDynamoDaoTest {

    private static final String CLASS_STUDENTS = "class-students";
    private static final String EMAIL = "email";
    private final String STUDENTS = "students";

    private final String HTTP_LOCALHOST = "http://localhost";

    private DynamoDBProxyServer server;
    private String port;
    private AmazonDynamoDB dynamodb;

    @Before
    public void setUp() throws Exception {
        System.setProperty("sqlite4java.library.path", "native-libs");
        port = "8080"; //getAvailablePort();
        server = ServerRunner.createServerFromCommandLineArgs(
                new String[]{"-inMemory", "-port", port});
        server.start();

        dynamodb = new AmazonDynamoDBClient();
        dynamodb.setEndpoint(HTTP_LOCALHOST + ":" + port);
    }


    @Test
    public void shouldGetEmptyTables() {
        final ListTablesResult tables = dynamodb.listTables();
        final List<String> tableNames = tables.getTableNames();
        assertThat(tableNames, is(empty()));
    }

    @Test
    public void shouldCreateTableStudents() {
        final StudentDynamoDao dynamoDao = new StudentDynamoDao(dynamodb);
        dynamoDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);

        final ListTablesResult tables = dynamodb.listTables();

        final List<String> tableNames = tables.getTableNames();
        assertThat(tableNames, hasSize(1));
        assertThat(tableNames, hasItem(STUDENTS));
    }

    @Test
    public void shouldInsertSingleRow() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);

        List<Student> students = studentDao.getAllStudents();
        assertThat(students, hasSize(0));

        final Student s = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");
        studentDao.saveStudent(s);

        students = studentDao.getAllStudents();
        assertThat(students, hasSize(1));

        final Student savedStudent = students.get(0);
        assertThat(savedStudent.getEmail(), is(s.getEmail()));
        assertThat(savedStudent.getName(), is(s.getName()));
        assertThat(savedStudent.getSurname(), is(s.getSurname()));
    }

    @Test
    public void shouldInsertMultipleRows() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);

        final Student s = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");
        studentDao.saveStudent(s);

        final Student s1 = aStudent("supriya.patil@cdk.com", "Supriya", "Patil");
        studentDao.saveStudent(s1);

        final List<Student> students = studentDao.getAllStudents();
        assertThat(students, hasSize(2));
    }

    @Test
    public void shouldFindStudentByEmail() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);

        final Student s = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");
        studentDao.saveStudent(s);

        final Student s1 = aStudent("supriya.patil@cdk.com", "Supriya", "Patil");
        studentDao.saveStudent(s1);

        final Optional<Student> matchingStudent = studentDao.findByEmail("injulkarnilesh@gmail.com");
        assertTrue(matchingStudent.isPresent());
        assertThat(matchingStudent.get(), is(s));
    }

    @Test
    public void shouldNotFindStudentByEmail() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);

        final Student s = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");
        studentDao.saveStudent(s);

        final Optional<Student> matchingStudent = studentDao.findByEmail("fake@gmail.com");
        assertFalse(matchingStudent.isPresent());
    }

    @Test
    public void shouldSaveWithHashAndRangeKeyAndFindWithHashKey() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(CLASS_STUDENTS,
                "class", ScalarAttributeType.S,
                EMAIL, ScalarAttributeType.S);

        final Student in = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");

        final Student sp = aStudent("patil.supriya@cdk.com", "Supriya", "Patil");

        final Student rk = aStudent("kumar.rohit@cdk.com", "Rohit", "Kumar");

        final Student st = aStudent("tarakar.sapan@cdk.com", "Sapan", "Tarkar");

        studentDao.saveStudentsInClass("Economics", in, st);
        studentDao.saveStudentsInClass("Physics", rk, sp, in);

        final List<Student> physicsStudents = studentDao.getAllStudentsOfClass("Physics");
        assertThat(physicsStudents, hasSize(3));

        final List<Student> economicStudents = studentDao.getAllStudentsOfClass("Economics");
        assertThat(economicStudents, hasSize(2));

        final List<Student> chemStudents = studentDao.getAllStudentsOfClass("Chemistry");
        assertThat(chemStudents, hasSize(0));
    }

    @Test
    public void shouldSaveWithHashAndRangeKeyAndFindWithHashAndRangeKey() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(CLASS_STUDENTS,
                "class", ScalarAttributeType.S,
                EMAIL, ScalarAttributeType.S);

        final Student in = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");

        final Student sp = aStudent("patil.supriya@cdk.com", "Supriya", "Patil");

        final Student rk = aStudent("kumar.rohit@cdk.com", "Rohit", "Kumar");

        final Student st = aStudent("tarakar.sapan@cdk.com", "Sapan", "Tarkar");

        studentDao.saveStudentsInClass("Economics", in, st);
        studentDao.saveStudentsInClass("Physics", rk, sp, in);

        final Optional<Student> student = studentDao.getStudentOfClass("Physics", "injulkarnilesh@gmail.com");
        assertTrue(student.isPresent());
        assertThat(student.get(), is(in));

        final Optional<Student> nonExistingStudent = studentDao.getStudentOfClass("Economics", "fake@gmail.com");
        assertFalse(nonExistingStudent.isPresent());
    }

    @Test
    public void shouldSupportTransactionWithSuccess() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);
        studentDao.createTable(CLASS_STUDENTS,
                "class", ScalarAttributeType.S,
                EMAIL, ScalarAttributeType.S);

        final Student in = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");

        studentDao.saveStudentsInClassTx("C++", in);

        final List<Student> students = studentDao.getAllStudents();
        assertThat(students, hasSize(1));

        final List<Student> cppStudents = studentDao.getAllStudentsOfClass("C++");
        assertThat(cppStudents, hasSize(1));
    }

    @Test
    public void failsToSaveNullClassName() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);
        studentDao.createTable(CLASS_STUDENTS,
                "class", ScalarAttributeType.S,
                EMAIL, ScalarAttributeType.S);

        final Student in = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");

        studentDao.saveStudent(in);
        try {
            studentDao.saveStudentsInClass(null, in);
        } catch (Exception e) {

        }

        final List<Student> allStudents = studentDao.getAllStudents();
        assertThat("Student is saved", allStudents, hasSize(1));
    }

    @Test
    public void saveInTransaction() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);
        studentDao.createTable(CLASS_STUDENTS,
                "class", ScalarAttributeType.S,
                EMAIL, ScalarAttributeType.S);

        final Student in = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");

        try {
            studentDao.saveStudentsInClassTx(null, in);
        } catch (Exception e) {

        }

        final List<Student> allStudents = studentDao.getAllStudents();
        assertThat("Transaction should not commit student", allStudents, hasSize(0));
    }


    @Test
    public void shouldUpdateStudent() {
        final StudentDynamoDao studentDao = new StudentDynamoDao(dynamodb);
        studentDao.createTable(STUDENTS, EMAIL, ScalarAttributeType.S);

        final Student s = aStudent("injulkarnilesh@gmail.com", "Nilesh", "Injulkar");
        studentDao.saveStudent(s);

        s.setName("Dr");
        s.setSurname("Watson");

        studentDao.updateStudent(s);

        final Optional<Student> updatedStudent = studentDao.findByEmail(s.getEmail());
        assertTrue(updatedStudent.isPresent());
        final Student updated = updatedStudent.get();
        assertThat(updated.getName(), is(s.getName()));
        assertThat(updated.getSurname(), is(s.getSurname()));
    }

    @After
    public void shutdown() throws Exception {
        server.stop();
    }

    private Student aStudent(final String email, final String name, final String surname) {
        final Student s = new Student();
        s.setEmail(email);
        s.setName(name);
        s.setSurname(surname);
        return s;
    }

    private static String getAvailablePort() throws IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        return String.valueOf(serverSocket.getLocalPort());
    }
}
