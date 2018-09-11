package com.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SqlQuery {
    public static void sqlFromParquet(SparkSession spark, String EventPath) throws AnalysisException {
        // Read a parquet and select from it
        Dataset<Row> countDF =
                spark.sql("select round(100*count(distinct case when " +
                        "cast(app.time as date) between date_add(reg.time,8-date_format(reg.time, 'u')) " +
                        "and date_add(reg.time,14-date_format(reg.time, 'u')) " +
                        "then reg.email else null end) " +
                        "/count(distinct reg.email),2) as prc " +
                        "from parquet.`" + EventPath + "/app_loaded.parquet` as app " +
                        "join parquet.`" + EventPath + "/registered.parquet` as reg " +
                        "on app.email = reg.email ");

        countDF.show();
    }
}