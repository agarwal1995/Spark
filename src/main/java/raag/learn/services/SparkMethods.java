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
import static org.apache.spark.sql.functions.expr;

public class SparkMethods {

    public static void main(String[] args) {
        SparkMethods sparkMethods = new SparkMethods();

        SparkSession sparkSession = SessionCreate.createSparkSession("SparkMethods", "local[*]");
        Dataset<Row> flightDataset = DatasetReader.readFromCSV(sparkSession, "csv", true, FilePathConstant.FLIGHT_DATA_2015_CSV.getPath(), null);

//        sparkMethods.take(flightDataset);
//        sparkMethods.sort(sparkSession);
//        sparkMethods.getNumPartitions(flightDataset);
//        sparkMethods.explainPlan(sparkSession);
//        sparkMethods.descMethod(flightDataset);
//        sparkMethods.selectMethod(flightDataset);
        sparkMethods.selectExprMethod(flightDataset);
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
     * -------------------------------------------------------------------------------------------
     * dataset.col()
     * to fetch the specific col from the dataframe
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

    /**
     * select(strings...)
     * Select method is used to give the results equivalent to SQL select statements
     */
    public void selectMethod(Dataset<Row> dataset) {
        dataset.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show();
        dataset.select(dataset.col("DEST_COUNTRY_NAME")).show();
        dataset.select(dataset.col("DEST_COUNTRY_NAME"), dataset.col("ORIGIN_COUNTRY_NAME")).show();

        // It will throw error as you have to give either both all object or column strings
//        dataset.select(dataset.col("DEST_COUNTRY_NAME"), "ORIGIN_COUNTRY_NAME").show();

        // will throw error as column does not exist
//        dataset.select(dataset.col("23")).show();
    }

    /**
     * selectExpr is shorthand for select(expr)
     */
    public void selectExprMethod(Dataset<Row> dataset) {
        dataset.select(expr("DEST_COUNTRY_NAME as DEST"), expr("ORIGIN_COUNTRY_NAME as Origin"), expr("DEST == ORIGIN as Res" )).show();

        // with selectExpr shorthand
        dataset.selectExpr("DEST_COUNTRY_NAME as DEST", "ORIGIN_COUNTRY_NAME as Origin", "DEST == ORIGIN as Result").show();
    }
}
