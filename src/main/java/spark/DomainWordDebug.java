package spark;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;

public class DomainWordDebug {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN);
        SparkConf sc = new SparkConf().setAppName("DomainWordDebug").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_domain_word_debug");
//        SparkConf sc = new SparkConf().setAppName("DomainWord").setMaster("local").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_domain_word");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> inputFile = jsc.textFile("hdfs://cloud" + args[0]);
//        JavaRDD<String> inputFile = jsc.textFile("/Users/crash/Downloads/test.txt");

        JavaRDD<Document> documents = inputFile.filter(text -> {
            JSONObject testJson = JSONObject.parseObject(text);
            String queryText = testJson.getString("query_text");
            return queryText.contains("芈月传");
        }).mapToPair(text -> {
            JSONObject textJson = JSONObject.parseObject(text);
            String queryText = textJson.getString("query_text");
            String domain = textJson.getString("return_domain");
            return new Tuple2<>(domain+"|"+queryText, 1);
        }).reduceByKey(Integer::sum).map(result -> {
            String[] keys = result._1.split("\\|");
            return new Document()
                    .append("query_text", keys[1])
                    .append("domain", keys[0])
                    .append("count", result._2);
        });

        // 存入mongo
        MongoSpark.save(documents);
    }
}
