package learn.s3lambda.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@DynamoDbBean
public class Department {
    private Integer departmentNumber;

    private String departmentName;

    private String location;

}
