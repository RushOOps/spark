package spark;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;
import util.StringUtil;

import java.util.ArrayList;
import java.util.List;

public class DomainMostRequest {

    private static final List<String> DOMAIN = new ArrayList<String>(){
        {
            add("VR");
            add("REMINDER");
            add("POEM");
            add("JOKE");
            add("STORY");
            add("IDIOM");
            add("WorldRecords");
            add("HTWHYS");
            add("HISTORY");
            add("DISEASE");
        }
    };

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("DomainMostRequest_08").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_domain_most_request");
//        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("BaikeUrl").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic_data_raw.BaikeUrl");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = jsc.textFile("hdfs://cloud" + args[0]);
//        JavaRDD<String> inputFile = jsc.textFile("/Users/crash/Desktop/test");
        JavaRDD<Document> doc = inputFile
                .map(JSONObject::parseObject)
                .filter(r -> {
                    String domain = r.getString("return_domain");
                    return StringUtil.isNotEmpty(domain) && DOMAIN.contains(domain)
                            && StringUtil.isNotEmpty(r.getString("query_text"));
                })
                .mapToPair(r -> new Tuple2<>(r.getString("query_text")
                        + "@@@" + r.getString("return_domain")
                        + "@@@" + r.getString("return_intent")
                        + "@@@" + r.getString("return_semantic"), 1))
                .reduceByKey(Integer::sum)
                .map(r -> {
                    String[] key = r._1.split("@@@");
                    return new Document().append("query_text", key[0])
                            .append("domain", key[1])
                            .append("intent", key[2])
                            .append("semantic", JSONObject.parseObject(key[3]))
                            .append("count", r._2);
                });
        MongoSpark.save(doc);
    }
}
