
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.trim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class TopicTrendAnalyzer {

    // Load all CSV batches and prepare the data
    public static Dataset<Row> loadData(SparkSession spark) {

        String inputPath =
                "hdfs://master:9000/hadoop/tweet_topics/*.csv";

        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .csv(inputPath);

        Dataset<Row> cleanData = data
                .dropDuplicates("id")
                .filter(col("normalized_topic").isNotNull())
                .filter(trim(col("normalized_topic")).notEqual(""))
                .filter(col("date").isNotNull());

        return cleanData
                .withColumn(
                        "calendar_day",
                        substring(col("date"), 1, 10)
                )
                .withColumn(
                        "calendar_hour",
                        substring(col("date"), 12, 2)
                );
    }



    // Global ranking
    public static Dataset<Row> getGlobalTrends(Dataset<Row> data, int limit) {
                Dataset<Row> trends =  data
                                        .groupBy("normalized_topic")
                                        .count()
                                        .orderBy(
                                                col("count").desc(),
                                                col("normalized_topic").asc()
                                        );

                if (limit > 0) {
                trends = trends.limit(limit);
                }

                return trends.select(
                        col("normalized_topic").alias("topic"),
                        col("count").alias("frequency")
                );
    }


    // Daily ranking
    public static Dataset<Row> getDailyTrends(Dataset<Row> data, String date, int limit) {

        Dataset<Row> trends =  data
                .filter(col("calendar_day").equalTo(date))
                .groupBy("normalized_topic")
                .count()
                .orderBy(
                        col("count").desc(),
                        col("normalized_topic").asc()
                );


        if (limit > 0) {
        trends = trends.limit(limit);
        }

        return trends.select(
                col("normalized_topic").alias("topic"),
                col("count").alias("frequency")
        );



        
    }


    // Hourly ranking
    public static Dataset<Row> getHourlyTrends(
            Dataset<Row> data, String date, int hour, int limit) {

        if (hour < 0 || hour > 23) {
            throw new IllegalArgumentException(
                    "Hour must be between 0 and 23."
            );
        }

        String formattedHour = String.format("%02d", hour);

        Dataset<Row> trends =  data
                .filter(col("calendar_day").equalTo(date))
                .filter(col("calendar_hour").equalTo(formattedHour))
                .groupBy("normalized_topic")
                .count()
                .orderBy(
                        col("count").desc(),
                        col("normalized_topic").asc()
                );
             

        if (limit > 0) {
        trends = trends.limit(limit);
        }

        return trends.select(
                col("normalized_topic").alias("topic"),
                col("count").alias("frequency")
        );
    }





        // Export any Spark DataFrame to CSV
        public static void exportToCsv(Dataset<Row> data, String outputPath) {
                data.write()
                        .option("header", "true")
                        .mode(SaveMode.Overwrite)
                        .csv(outputPath);

                System.out.println("Export completed: " + outputPath);
        }

    



        public static void main(String[] args) {

                if (args.length < 1 || args.length > 5) {
                        System.out.println("Usage:");
                        System.out.println("  Global: TopicTrendAnalyzer global [limit] [outputPath]");
                        System.out.println("  Daily:  TopicTrendAnalyzer daily <date> [limit] [outputPath]");
                        System.out.println("  Hourly: TopicTrendAnalyzer hourly <date> <hour> [limit] [outputPath]");
                        return;
                }

                SparkSession spark = SparkSession.builder()
                        .appName("TopicTrendAnalyzer")
                        .master("local[*]")
                        .getOrCreate();

                spark.sparkContext().setLogLevel("WARN");

                try {
                        Dataset<Row> data = loadData(spark);
                        Dataset<Row> trends;

                        String mode = args[0].toLowerCase();
                        int limit = 20;
                        String outputPath = null;

                        switch (mode) {

                        case "global":
                                if (args.length > 3) {
                                throw new IllegalArgumentException(
                                        "Global accepts an optional limit and output path.");
                                }

                                if (args.length >= 2) {
                                limit = Integer.parseInt(args[1]);
                                }

                                if (args.length == 3) {
                                outputPath = args[2];
                                }

                                trends = getGlobalTrends(data, limit);
                                break;

                        case "daily":
                                if (args.length < 2 || args.length > 4) {
                                throw new IllegalArgumentException(
                                        "Daily requires a date, optional limit and output path.");
                                }

                                if (args.length >= 3) {
                                limit = Integer.parseInt(args[2]);
                                }

                                if (args.length == 4) {
                                outputPath = args[3];
                                }

                                trends = getDailyTrends(data, args[1], limit);
                                break;

                        case "hourly":
                                if (args.length < 3 || args.length > 5) {
                                throw new IllegalArgumentException(
                                        "Hourly requires a date, hour, optional limit and output path.");
                                }

                                int hour = Integer.parseInt(args[2]);

                                if (args.length >= 4) {
                                limit = Integer.parseInt(args[3]);
                                }

                                if (args.length == 5) {
                                outputPath = args[4];
                                }

                                trends = getHourlyTrends(data, args[1], hour, limit);
                                break;

                        default:
                                throw new IllegalArgumentException(
                                        "Mode must be global, daily, or hourly.");
                        }

                        System.out.println("===== TREND RESULTS =====");
                        trends.show(limit, false);

                        if (outputPath != null) {
                        exportToCsv(trends, outputPath);
                        }

                } finally {
                        spark.stop();
                }
        }

    
}
