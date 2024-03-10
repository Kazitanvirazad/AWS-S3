package learn.s3lambda.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import learn.s3lambda.model.Employee;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class S3TriggerHandler implements RequestHandler<S3Event, Void> {
    String REGION = System.getenv("REGION");
    String TABLE_NAME = System.getenv("TABLE_NAME");

    @Override
    public Void handleRequest(S3Event s3Event, Context context) {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<List<Employee>> typeReference = new TypeReference<>() {
        };
        TableSchema<Employee> employeeTableSchema = TableSchema.fromBean(Employee.class);
        StringBuilder objectPayload = new StringBuilder();
        Region region = Region.of(REGION);
        String bucket;
        String key;

        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            S3EventNotification.S3Entity s3Entity = record.getS3();
            if (s3Entity != null && s3Entity.getBucket() != null && s3Entity.getObject() != null) {
                bucket = s3Entity.getBucket().getName();
                key = s3Entity.getObject().getKey();

                GetObjectRequest getObjectRequest = GetObjectRequest.builder().
                        bucket(bucket)
                        .key(key).build();

                try (DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(region).build();
                     S3Client s3Client = S3Client.builder().region(region).build();
                     ResponseInputStream<GetObjectResponse> objectInputStream = s3Client.getObject(getObjectRequest);
                     BufferedReader reader = new BufferedReader(new InputStreamReader(objectInputStream))) {

                    DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();

                    reader.lines().forEach(objectPayload::append);

                    String employeeJson = objectPayload.toString();


                    List<Employee> employeeList = mapper.readValue(employeeJson, typeReference);

                    objectPayload = new StringBuilder();

                    for (Employee employee : employeeList) {
                        enhancedClient.table(TABLE_NAME, employeeTableSchema).putItem(employee);
                    }
                } catch (IOException | UnsupportedOperationException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        return null;
    }
}
