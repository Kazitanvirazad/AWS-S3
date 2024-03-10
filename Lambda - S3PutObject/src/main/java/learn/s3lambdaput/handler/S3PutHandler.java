package learn.s3lambdaput.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import learn.s3lambdaput.model.Employee;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.ArrayList;
import java.util.List;

public class S3PutHandler {
    String REGION = System.getenv("REGION");
    String TABLE_NAME = System.getenv("TABLE_NAME");
    String BUCKET_NAME = System.getenv("BUCKET_NAME");

    public void handleRequest() {
        ObjectMapper mapper = new ObjectMapper();

        TableSchema<Employee> employeeTableSchema = TableSchema.fromBean(Employee.class);
        Region region = Region.of(REGION);

        List<Employee> employeeList = new ArrayList<>();

        try (DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(region).build();
             S3Client s3Client = S3Client.builder().region(region).build()) {

            ScanEnhancedRequest scanEnhancedRequest = ScanEnhancedRequest.builder()
                    .build();

            DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                    .dynamoDbClient(dynamoDbClient).build();

            enhancedClient.table(TABLE_NAME, employeeTableSchema)
                    .scan(scanEnhancedRequest).items().stream()
                    .forEach(employeeList::add);

            String employeesJson = mapper.writeValueAsString(employeeList);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key("employeeList.json")
                    .contentType("application/json")
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromString(employeesJson));
        } catch (JsonProcessingException | UnsupportedOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
