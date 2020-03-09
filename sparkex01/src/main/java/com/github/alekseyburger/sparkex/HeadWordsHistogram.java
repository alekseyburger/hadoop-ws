package com.github.alekseyburger.sparkex;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.slf4j.LoggerFactory.*;


/*
 It reads file names list from the file and counts the file number in root subdirectorues
 Exec: bin/spark-submit --class "com.github.alekseyburger.sparkex.HeadWordsHistogram" --master yarn /home/alekseyb/bigdata-workspace/sparkex01/target/sparkex-1.0-SNAPSHOT.jar hdfs://lenovo:9000/tmp/testing.txt
 */
public class HeadWordsHistogram {

    protected static final Logger logger = getLogger(HeadWordsHistogram.class);

    public static void main(String[] argv) {

        String inputFileName = "hdfs://lenovo:9000/tmp/testing.txt";
        if (argv.length ==  0) {
            System.out.println("Provide hdfs file name as parameter!");
            System.exit(1);
        }

        inputFileName =  argv[0];
        logger.info("Start file {} processing", inputFileName);

        SparkConf conf = new SparkConf().setAppName("Word_Histogram");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> textLoadRDD = ctx.textFile(inputFileName);
        JavaRDD<String> arrayRDD = textLoadRDD.flatMap(content -> Arrays.asList(content.split("\\W+")).iterator());
        JavaPairRDD<String, Integer> kvRDD = arrayRDD.mapToPair(word -> new Tuple2(word, 1));
        JavaPairRDD<String, Integer> counters = kvRDD.reduceByKey((v1, v2) -> (v1+v2));
        JavaPairRDD<Integer, String> result =  counters.mapToPair(Tuple2::swap)
                .sortByKey(false);

        // collect RDD for printing
        System.out.println("HeadWordsHistogram result:");
        for(Tuple2 line: result.collect() ){
            System.out.println(line);
        }

    }
}
