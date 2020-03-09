package com.github.alekseyburger.sparkex;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;


/*
 It reads file names list from the file and counts the file number in root subdirectorues
 Exec: bin/spark-submit --class "com.github.alekseyburger.sparkex.HeadWordsHistogram" --master local[2] /home/alekseyb/bigdata-workspace/sparkex01/target/sparkex-1.0-SNAPSHOT.jar
 */
public class HeadWordsHistogram {
    public static void main(String[] argv) {

        SparkConf conf = new SparkConf().setAppName("Word_Histogram");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> textLoadRDD = ctx.textFile("/home/alekseyb/spark-2.3.1-bin-hadoop2.7/fs.md");
        JavaRDD<String> arrayRDD = textLoadRDD.map(line -> {
             String[] words = line.split("/");
             return ( words.length > 2 ? words[1] : ""); })
                .filter(string -> string.length()>0);
        JavaPairRDD<String, Integer> kvRDD = arrayRDD.mapToPair(word -> new Tuple2(word, 1));
        JavaPairRDD<String, Integer> counters = kvRDD.reduceByKey((v1, v2) -> (v1+v2));
        JavaPairRDD<Integer, String> result =  counters.mapToPair(Tuple2::swap)
                .sortByKey(false);

        // .flatMap(WordsUtil::getWords)
        // .mapToPair(word -> new Tuple2(word, 1))
        // .reduceByKey((a,b)->a+b)
        // .mapToPair(Tuple2::swap)
        // .sortByKey(False).map(Tupla2::_2).tale(x)

        // collect RDD for printing
        for(Tuple2 line: result.collect() ){
            System.out.println(line);
        }

    }
}
