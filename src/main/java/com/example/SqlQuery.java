package com.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlQuery {
    public static void sqlFromParquet(SparkSession spark, String EventPath) throws AnalysisException {
        // Read a parquet and select from it
        Dataset<Row> countDF =
                spark.sql("select round(100*count(distinct case when datediff(app.time,reg.time) between 0 and 7 " +
                        "then reg.email else null end)/count(distinct reg.email),2) as prc " +
                        "from parquet.`" + EventPath + "/app_loaded.parquet` as app " +
                        "join parquet.`" + EventPath + "/registered.parquet` as reg " +
                        "on app.email = reg.email");

        countDF.show();
    }
}
