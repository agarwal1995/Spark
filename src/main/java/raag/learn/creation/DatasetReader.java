package raag.learn.creation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class DatasetReader {

    public void inferSchemaExample(SparkSession sparkSession, String filePath, StructType schema) {
        Dataset<Row> schemaInferTrue = sparkSession.read().format("csv").option("header", true).option("inferSchema", true).csv(filePath);
        Dataset<Row> schemaInferFalse= sparkSession.read().format("csv").option("header", true).option("inferSchema", false).csv(filePath);
        Dataset<Row> schemaInferTrueWithSchemaGiven = sparkSession.read().format("csv").option("header", true).option("inferSchema", true).schema(schema).csv(filePath);
        Dataset<Row> schemaInferFalseWithSchemaGiven = sparkSession.read().format("csv").option("header", true).option("inferSchema", false).schema(schema).csv(filePath);

        schemaInferTrue.printSchema();
        schemaInferFalse.printSchema();
        schemaInferTrueWithSchemaGiven.printSchema();
        schemaInferFalseWithSchemaGiven.printSchema();

        schemaInferTrue = sparkSession.read().format("csv").option("header", false).option("inferSchema", true).csv(filePath);
        schemaInferFalse= sparkSession.read().format("csv").option("header", false).option("inferSchema", false).csv(filePath);
        schemaInferTrueWithSchemaGiven = sparkSession.read().format("csv").option("header", false).option("inferSchema", true).schema(schema).csv(filePath);
        schemaInferFalseWithSchemaGiven = sparkSession.read().format("csv").option("header", false).option("inferSchema", false).schema(schema).csv(filePath);

        schemaInferTrue.printSchema();
        schemaInferFalse.printSchema();
        schemaInferTrueWithSchemaGiven.printSchema();
        schemaInferFalseWithSchemaGiven.printSchema();;
    }


    public static Dataset<Row> readFromCSV(SparkSession sparkSession, String format, boolean inferSchema, String filePath, StructType schema) {
        return sparkSession
                .read()
                .format(format)
                .option("header", true)
                .option("inferSchema", inferSchema)
                .schema(schema)
                .csv(filePath);
    }
}
