package raag.learn.services;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import raag.learn.creation.DatasetReader;
import raag.learn.utility.FilePathConstant;

public class SparkAsSql {
    public static void main(String[] args) throws AnalysisException {
        SparkAsSql sparkAsSql = new SparkAsSql();

        SparkSession sparkSession = SparkSession.builder().appName("SparkAsSql").master("local[*]").getOrCreate();
        Dataset<Row> dataset = DatasetReader.readFromCSV(sparkSession,"csv", true, FilePathConstant.FLIGHT_DATA_2015_CSV.getPath(), null);

        sparkAsSql.groupByQuery(sparkSession, dataset);
    }

    /**
     * createOrReplaceTempView(string)
     * creates a temp view/table in the sparkSession sqlContext, so sql queries could be applied to it for the given business logic
     *
     * sparkSession.sql()
     * function to apply sql query for the given business logic
     */
    public void groupByQuery(SparkSession sparkSession, Dataset<Row> dataset) throws AnalysisException {

        // create a table from the dataset
        dataset.createOrReplaceTempView("flight_dataset");

        // In sql query way
        Dataset<Row> sqlDataset = sparkSession.sql("Select DEST_COUNTRY_NAME, sum(count) from flight_dataset GROUP BY DEST_COUNTRY_NAME");
        sqlDataset.show();

        // In dataframe way
        Dataset<Row> dfDataset = dataset.groupBy("DEST_COUNTRY_NAME").sum("count");
        dfDataset.show();
    }

    /**
     *
     */
    public void findTopFiveDestinationCountryInFlightDataset(SparkSession sparkSession, Dataset<Row> dataset) {
        // create a table from the dataset
        dataset.createOrReplaceTempView("flight_dataset");

        // sql way
        Dataset<Row> sqlDataset = sparkSession.sql("Select DEST_COUNTRY_NAME, sum(count) as destination_total FROM flight_dataset GROUP BY DEST_COUNTRY_NAME ORDER BY Sum(count) limit 5");
        sqlDataset.show();
    }
}
