package learn.s3lambda.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@DynamoDbBean
@JsonIgnoreProperties(ignoreUnknown = true)
public class Employee {
    private Integer employeeNumber;

    private String employeeName;

    private String designation;

    private Integer manager;

    private String hireDate;

    private Integer salary;

    private Integer commission;

    private Department department;

    @DynamoDbPartitionKey
    public Integer getEmployeeNumber() {
        return employeeNumber;
    }
}
