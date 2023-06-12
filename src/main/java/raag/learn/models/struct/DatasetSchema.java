package raag.learn.models.struct;


import org.apache.spark.sql.types.StructType;

public class DatasetSchema {
    public static StructType getEmployeeSchema() {
        StructType schema = new StructType()
                .add("EmployeeId", "Integer", false)
                .add("FirstName", "String", true)
                .add("LastName", "String", true)
                .add("Email", "String", true)
                .add("PhoneNumber", "String", true)
                .add("HireDate","Date", true)
                .add("JobId", "String", true)
                .add("Salary", "Integer", true)
                .add("Commission_Percentage", "Integer", true)
                .add("ManagerId", "Integer", true)
                .add("DepartmentId", "Integer", true);
        return schema;
    }
}
