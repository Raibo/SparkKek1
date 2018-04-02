import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.functions.col;

public class MakeParquet {
    public static void main(String[] args) {
        String events = "D:\\Coding\\Java\\RTB_MEGA_FOLDER\\events.json";
        String parqAppLoaded = "D:\\Coding\\Java\\RTB_MEGA_FOLDER\\app_loaded.parquet";
        String parqRegistered = "D:\\Coding\\Java\\RTB_MEGA_FOLDER\\registered.parquet";

        SparkSession sparkSes = SparkSession.builder()
                .appName("RTB_Test")
                .getOrCreate();

        //----------- app_loaded
        Dataset<Row> datasetAL = sparkSes.read().json(events)
                .filter(col("_n").equalTo("app_loaded"));

        datasetAL = datasetAL.select(
                datasetAL.col("_t").cast("timestamp").as("time"),
                datasetAL.col("_p").as("email"),
                datasetAL.col("device_type")
                );

        datasetAL.write().mode(SaveMode.Overwrite).parquet(parqAppLoaded);


        //----------- registered
        Dataset<Row> datasetReg = sparkSes.read().json(events)
                .filter(col("_n").equalTo("registered"));
        datasetReg = datasetReg.select(
                datasetReg.col("_t").cast("timestamp").as("time"),
                datasetReg.col("_p").as("email"),
                datasetReg.col("channel")
        );

        datasetReg.write().mode(SaveMode.Overwrite).parquet(parqRegistered);
    }
}
