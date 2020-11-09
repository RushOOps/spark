package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class HelloWorld_java {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("hello_java");
        conf.setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(new SparkContext(conf));
        JavaRDD<String> lines = jsc.textFile("file:///Users/crash/Desktop/text/memo");
        JavaRDD<String> words = lines.flatMap( line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> result = pairs.reduceByKey(Integer::sum);
        result.foreach(res -> System.out.println(res._1+":"+res._2));
        jsc.close();
    }
}
