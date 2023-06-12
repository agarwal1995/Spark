package raag.learn.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import raag.learn.creation.DatasetReader;
import raag.learn.creation.SessionCreate;
import raag.learn.utility.FilePathConstant;

public class SparkMethods {

    public static void main(String[] args) {
        SparkMethods sparkMethods = new SparkMethods();

        SparkSession sparkSession = SessionCreate.createSparkSession("SparkMethods", "local[*]");
        Dataset<Row> flightDataset = DatasetReader.readFromCSV(sparkSession, "csv", true, FilePathConstant.FLIGHT_DATA_2015_CSV.getPath(), null);

        sparkMethods.take(flightDataset);
    }

    /**
     * take the n element form the dataset into an array
     */
    public void take(Dataset<Row> dataset) {
        Object[] objects = (Object[]) dataset.take(3);
        for (Object object: objects) {
            System.out.println(object);
        }
    }
}
