package in.injulkar.nilesh.aws.story;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StudentLambdaIntegrationTest {

    @Test
    public void shouldLoadAllStudents() {
        final StudentsLambda lambda = new StudentsLambda();
        final List<Student> allCustomers = lambda.getAllCustomers(null, null);
        Assert.assertTrue(allCustomers.size() > 0);
    }
}
