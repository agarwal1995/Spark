package raag.learn.creation;

import org.apache.spark.sql.SparkSession;

public class SessionCreate {
    public static SparkSession createSparkSession(String appName, String master) {
        return SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();
    }

}
