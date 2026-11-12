
package recommendation;

import org.apache.spark.sql.SparkSession;

public final class SparkFactory {

    private SparkFactory() {}

    public static SparkSession create(String appName) {
        SparkSession spark = SparkSession.builder()
                .appName(appName)
                .master("local[2]")
                .config("spark.driver.memory", "768m")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.default.parallelism", "2")
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.driver.extraJavaOptions",
                        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED " +
                        "--add-opens=java.base/java.nio=ALL-UNNAMED")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        return spark;
    }
}
