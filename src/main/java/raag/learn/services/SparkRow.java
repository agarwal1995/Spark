package raag.learn.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import raag.learn.creation.DatasetReader;
import raag.learn.utility.FilePathConstant;

/**
 * @author raag
 */
public class SparkRow {


    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("SparkRow").master("local[*]").getOrCreate();
        Dataset<Row> dataset = DatasetReader.readFromCSV(sparkSession, "csv", true, FilePathConstant.FLIGHT_DATA_2015_CSV.getPath(), null);

        SparkRow sparkRow = new SparkRow();

        sparkRow.rowInfo(dataset);
    }


    /**
     * first()
     * fetches the first Row from the dataset
     * --------------------------------------
     * apply(n)
     * fetches the element of the nth column, irrespective of datatype of the column
     */
    public void rowInfo(Dataset<Row> dataset) {
        Row row = dataset.first();
        System.out.println(row.toString());

        System.out.println(row.apply(1));
    }
}
