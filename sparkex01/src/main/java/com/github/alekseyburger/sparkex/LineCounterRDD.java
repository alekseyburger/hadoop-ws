package com.github.alekseyburger.sparkex;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
 Exec: bin/spark-submit --class "com.github.alekseyburger.sparkex.LineCounterRDD" --master local[2] /home/alekseyb/bigdata-workspace/sparkex01/target/sparkex-1.0-SNAPSHOT.jar
 */
public class LineCounterRDD {

    public static void main(String[] argv) {
        SparkConf conf = new SparkConf().setAppName("Line_Count");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> textLoadRDD = ctx.textFile("/home/alekseyb/spark-2.3.1-bin-hadoop2.7/README.md");
        System.out.println(textLoadRDD.count());
    }
}
