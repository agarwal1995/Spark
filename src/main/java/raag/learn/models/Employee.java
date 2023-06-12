package raag.learn.models;

import lombok.Data;

import java.util.Date;

@Data
public class Employee {
    private Integer employeeId;
    private String firstName;
    private String lastName;
    private String email;
    private String phoneNumber;
    private Date hireDate;
    private String jobId;
    private Integer salary;
    private String commissionPCT;
    private String managerId;
    private String departmentId;
}
