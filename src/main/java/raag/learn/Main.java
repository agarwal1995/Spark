package raag.learn;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import raag.learn.creation.DatasetReader;
import raag.learn.models.struct.DatasetSchema;

public class Main {

    DatasetReader datasetReader = new DatasetReader();

    String employeeFilePath = "/home/raag/Study/github/Spark/src/main/resources/csv/employees.csv";

    public static void main(String[] args) throws Exception {
        Main main = new Main();

        SparkSession sparkSession = SparkSession.builder().appName("Spark").master("local[*]").getOrCreate();

        // InferSchema
        main.inferSchema(sparkSession);

    }

    /**
     * In case of null schema when schema is used while creating dataset it will not give any error and perform the operation as no schema is being given like
     */
    public void inferSchema(SparkSession sparkSession) {
        datasetReader.inferSchemaExample(sparkSession, employeeFilePath, DatasetSchema.getEmployeeSchema());
        Dataset<Row> dataset = datasetReader.readFromCSV(sparkSession, "csv", true, employeeFilePath, null);
        dataset.printSchema();
    }
}