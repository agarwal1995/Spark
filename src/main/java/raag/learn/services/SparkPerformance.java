package raag.learn.services;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;

/**
 * @author raag
 */
public class SparkPerformance {

    static Logger log;

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().appName("SparkPerformance").master("local[*]").getOrCreate();
        log = Logger.getLogger("SparkPerformance");
        String path1="/home/raag/Study/github/Spark/src/main/resources/json/activity-data.json";
        String path = "/home/raag/Study/github/Spark/src/main/resources/parquet/employees_3.parquet";
        Dataset<Row> dataset = sparkSession.read().option("header", true).option("inferSchema", true).parquet(path);
        log.info("Path :" + path);
//        dataset.printSchema();
//        log.info("Schema : ");
//        dataset.show(100);
//        log.info("Result :");
//        check(dataset, sparkSession);
        checkSparkAggrFilter(dataset);
    }

    public static void checkSparkAggrFilter(Dataset<Row> dataset) {
        dataset.printSchema();
        dataset.show();
        long count = dataset.count();
        System.out.println("count1 : " + count);
        count = dataset.filter(col("LAST_NAME")).count();
        System.out.println("count2 : " + count);
    }

    public static void check(Dataset<Row> dataset, SparkSession sparkSession) {
        Column col1 = col("FIRST_NAME").isNull().or(col("LAST_NAME").isNull());
        Column col2 = col("LAST_NAME").isNotNull();
        Column col3 = col("PHONE_NUMBER").isNotNull();
        Column col4 = col("MANAGER_ID").isNotNull();
        Column col5 = col("DEPARTMENT_ID").isNotNull();

        long time = System.currentTimeMillis();
        StringBuilder sbr = new StringBuilder();
        StringBuilder sb = new StringBuilder();
        sb.append("check1 :").append(dataset.filter(col1).count()).append("\n");
        sbr.append(" Time 1 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();
        sb.append("check2 :").append(dataset.filter(col2).count()).append("\n");
        sbr.append(" Time 2 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();
        sb.append("check3 :").append(dataset.filter(col3).count()).append("\n");
        sbr.append(" Time 3 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();
        sb.append("check4 :").append(dataset.filter(col4).count()).append("\n");
        sbr.append(" Time 4 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();
        sb.append("check5 :").append(dataset.filter(col5).count()).append("\n");
        sbr.append(" Time 5 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

    //    System.out.println(sbr.toString());

        Column[] columns = new Column[5];
        columns[0] = sum(when(col1, 1).otherwise(0)).alias("1");
        columns[1] = sum(when(col2, 1).otherwise(0)).alias("2");
        columns[2] = sum(when(col3, 1).otherwise(0)).alias("3");
        columns[3] = sum(when(col4, 1).otherwise(0)).alias("4");
        columns[4] = sum(when(col5, 1).otherwise(0)).alias("5");


        sbr.append(" Time 6 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();
        Dataset<Row> aggData = dataset.agg(columns[0], columns);
        sbr.append(" Time 7 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();
        aggData.show();
        sbr.append(" Time 8 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();
        aggData.show();
        sbr.append(" Time 9 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        aggData.show();
        Object obj1 = aggData.first().get(0);
        sbr.append(" Time 10 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Object obj2 = aggData.first().get(1);
        sbr.append(" Time 11 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Object obj3 = aggData.first().get(2);
        sbr.append(" Time 12 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Row row = aggData.first();
        sbr.append(" Time 13 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Object obj4 = row.get(0);
        sbr.append(" Time 14 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Object obj5 = row.get(1);
        sbr.append(" Time 15 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Object obj6 = row.get(2);
        sbr.append(" Time 16 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Dataset<Row> result = sparkSession.createDataFrame(Collections.singletonList(row), row.schema());
        result.printSchema();
        result.show();

        sbr.append(" Time 17 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Object obj7 = result.first().get(0);
        sbr.append(" Time 18 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Object obj8 = result.first().get(1);
        sbr.append(" Time 19 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        Object obj9 = result.first().get(2);
        sbr.append(" Time 20 :").append(System.currentTimeMillis() - time).append("\n");
        time = System.currentTimeMillis();

        System.out.println(sbr);
        System.out.println(sb);
    }
}
