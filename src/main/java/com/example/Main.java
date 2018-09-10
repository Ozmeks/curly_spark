package com.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) throws AnalysisException {
        // Initiate a session
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        if (args[0].equals("write") && (args.length == 3)) {
            com.example.JsonLoad.saveToParquet(spark, args[1], args[2]);
        } else if (args[0].equals("read") && (args.length == 2)) {
            SqlQuery.sqlFromParquet(spark, args[1]);
        }

        // Close the session
        spark.stop();
    }
}
