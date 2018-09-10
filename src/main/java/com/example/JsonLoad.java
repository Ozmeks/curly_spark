package com.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class JsonLoad {
    public static void saveToParquet(SparkSession spark, String ParquetPath, String JsonPath) throws AnalysisException {
        // Read json
        Dataset<Row> df = spark.read().json(JsonPath + "/events.json");

        // Save to a parquet with filter and cast
        df.filter(col("_n").equalTo("app_loaded")).
                select(col("_p").as("email"), col("_t").cast("timestamp").as("time"), col("device_type")).
                write().parquet(ParquetPath + "/app_loaded.parquet");

        df.filter(col("_n").equalTo("registered")).
                select(col("_p").as("email"), col("_t").cast("timestamp").as("time"), col("channel")).
                write().parquet(ParquetPath + "/registered.parquet");
    }
}