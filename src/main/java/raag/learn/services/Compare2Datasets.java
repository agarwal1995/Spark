package raag.learn.services;


import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;

import scala.collection.Iterable;
import scala.jdk.CollectionConverters;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * @author raag
 */
public class Compare2Datasets {

    private static final String ANTI_JOIN = "left_anti";

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Compare2Datasets").master("local[*]").getOrCreate();

        String firstCsvPath = "/home/raag/Study/github/Spark/src/main/resources/comparing/station_data_1.csv";
        String secondCsvPath = "/home/raag/Study/github/Spark/src/main/resources/comparing/station_data_2.csv";

        Dataset<Row> firstDataSet = sparkSession.read().format("csv").option("header", true).option("inferSchema", true).csv(firstCsvPath);
        Dataset<Row> secondDataSet = sparkSession.read().format("csv").option("header", true).option("inferSchema", true).csv(secondCsvPath);
        firstDataSet.printSchema();
        firstDataSet.show();
        secondDataSet.printSchema();
        secondDataSet.show();

        joinWithoutExcludedColumns(firstDataSet, secondDataSet);
//        joinWithAllColumns(firstDataSet, secondDataSet);
//        joinWithSomeColumns(firstDataSet, secondDataSet);
//        joinWithDifferentColNames(firstDataSet, secondDataSet);
    }

    private static void joinWithAllColumns(Dataset<Row> first, Dataset<Row> second) {
        List<String> list = Arrays.asList(second.columns());
        Iterable<String> scalaIterableList = CollectionConverters.IterableHasAsScala(list).asScala();
        Dataset<Row> firstOnlyDataset = first.join(second, scalaIterableList.toSeq(), ANTI_JOIN).withColumn("PresentIn", functions.lit("Prod"));
        Dataset<Row> secondOnlyDataset = second.join(first, scalaIterableList.toSeq(), ANTI_JOIN).withColumn("PresentIn", functions.lit("Stage"));
        firstOnlyDataset.printSchema();
        secondOnlyDataset.printSchema();
        firstOnlyDataset.show();
        secondOnlyDataset.show();
        Dataset<Row> result = firstOnlyDataset.unionAll(secondOnlyDataset);
        result.printSchema();
        result.show();
    }

    private static void joinWithSomeColumns(Dataset<Row> first, Dataset<Row> second) {
        String[] cols = {"station_id","name"};
        List<String> list = Arrays.asList(cols);
        Iterable<String> scalaIterableList = CollectionConverters.IterableHasAsScala(list).asScala();
        Dataset<Row> firstOnlyDataset = first.join(second, scalaIterableList.toSeq(), ANTI_JOIN).withColumn("PresentIn", functions.lit("Prod"));
        Dataset<Row> secondOnlyDataset = second.join(first, scalaIterableList.toSeq(), ANTI_JOIN).withColumn("PresentIn", functions.lit("Stage"));
        firstOnlyDataset.printSchema();
        secondOnlyDataset.printSchema();
        firstOnlyDataset.show();
        secondOnlyDataset.show();
        Dataset<Row> result = firstOnlyDataset.unionAll(secondOnlyDataset);
        result.printSchema();
        result.show();
    }

    public static void joinWithoutExcludedColumns(Dataset<Row> first, Dataset<Row> second) {
        String[] colsToExclude = {"station_id", "name"};
        List<String> list = Arrays.asList(colsToExclude);
        Set<String> set = new HashSet<>(list);
        String[] resultList = Arrays.stream(first.columns()).filter((str)-> !set.contains(str)).toArray(String[]::new);
        Iterable<String> scalaIterableList = CollectionConverters.IterableHasAsScala(Arrays.asList(resultList)).asScala();
        Dataset<Row> firstOnlyDataset = first.join(second, scalaIterableList.toSeq(), ANTI_JOIN).withColumn("PresentIn", functions.lit("Prod"));
        Dataset<Row> secondOnlyDataset = second.join(first, scalaIterableList.toSeq(), ANTI_JOIN).withColumn("PresentIn", functions.lit("Stage"));
        firstOnlyDataset.printSchema();
        secondOnlyDataset.printSchema();
        firstOnlyDataset.show();
        secondOnlyDataset.show();
        Dataset<Row> result = firstOnlyDataset.unionAll(secondOnlyDataset);
        result.printSchema();
        result.show();
    }

    private static void joinWithDifferentColNames(Dataset<Row> first, Dataset<Row> second) {
        Map<String, List<String>> map = new HashMap<>();
        map.put("df1", new ArrayList<>());
        map.put("df2", new ArrayList<>());
        map.get("df1").add("station_id");
        map.get("df1").add("lat");
        map.get("df2").add("station_id");
        map.get("df2").add("long");

        Column column1 = null;
        List<String> df1List = map.get("df1");
        List<String> df2List = map.get("df2");

        for (int i=0;i<df1List.size();i++) {
            if(column1!=null) {
                column1 = column1.and(first.col(df1List.get(i)).equalTo(second.col(df2List.get(i))));
                continue;
            }
            column1 = first.col(df1List.get(i)).equalTo(second.col(df2List.get(i)));
        }

        Dataset<Row> firstOnlyDataset = first.join(second, column1, ANTI_JOIN).withColumn("PresentIn", functions.lit("Prod"));
        Dataset<Row> secondOnlyDataset = second.join(first, column1, ANTI_JOIN).withColumn("PresentIn", functions.lit("Stage"));
        firstOnlyDataset.printSchema();
        secondOnlyDataset.printSchema();
        firstOnlyDataset.show();
        secondOnlyDataset.show();
        Dataset<Row> result = firstOnlyDataset.unionAll(secondOnlyDataset);
        result.printSchema();
        result.show();
    }
}
