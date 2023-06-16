package raag.learn.services;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import raag.learn.creation.DatasetReader;
import raag.learn.creation.SessionCreate;
import raag.learn.utility.FilePathConstant;

import javax.xml.crypto.Data;
import static org.apache.spark.sql.functions.desc;

public class SparkMethods {

    public static void main(String[] args) {
        SparkMethods sparkMethods = new SparkMethods();

        SparkSession sparkSession = SessionCreate.createSparkSession("SparkMethods", "local[*]");
        Dataset<Row> flightDataset = DatasetReader.readFromCSV(sparkSession, "csv", true, FilePathConstant.FLIGHT_DATA_2015_CSV.getPath(), null);

//        sparkMethods.take(flightDataset);
//        sparkMethods.sort(sparkSession);
//        sparkMethods.getNumPartitions(flightDataset);
//        sparkMethods.explainPlan(sparkSession);
        sparkMethods.descMethod(flightDataset);
    }

    /**
     * take(n)
     * take the n element form the dataset into an array
     */
    public void take(Dataset<Row> dataset) {
        Object[] objects = (Object[]) dataset.take(3);
        for (Object object: objects) {
            System.out.println(object);
        }
    }

    /**
     * sort(colStrings)
     * sort the dataset for the given columns string
     * sort(Columns)
     * sort the dataset for the given columns and can apply the sortBy order for each specific column
     */
    public void sort(SparkSession sparkSession) {
        Dataset<Row> numberDataset = sparkSession.range(1000).toDF("number");
        numberDataset.show();
        numberDataset.printSchema();
        Column column = numberDataset.col("number");
        Dataset<Row> reverseSorted = numberDataset.sort(column.desc());
        numberDataset.show();
        reverseSorted.show();
        reverseSorted.printSchema();
    }

    /**
     * explain()
     * Explain the logical plan that the spark has been building up for computation/transformation
     */
    public void explainPlan(SparkSession sparkSession) {
        Dataset<Row> numberDataset = sparkSession.range(1000).toDF("number");
        numberDataset.sort(numberDataset.col("number").desc()).explain();
    }

    /**
     * getNumPartitions()
     * get the current length/size of partitions
     */
    public void getNumPartitions(Dataset<Row> dataset) {
        System.out.println(dataset.rdd().getNumPartitions());
    }

    /**
     * desc("") static method used to sort in descending order
     */
    public void descMethod(Dataset<Row> dataset) {
        // Before Sorting
        dataset.show(10);

        // Sorting
        dataset = dataset.sort(desc("count"));

        // After Sorting
        dataset.show(10);
    }
}
