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

/**
 * 领域：APP  CONTROL  TV   VIDEO  CHAT  分开导5份数据
 * 时间：最近1个月
 * 字段：频次+  txt  sourceFlag domain intent semantic
 * 要求：按频次排序取前7w
 */

public class Test0 {

    private static final List<String> DOMAIN = new ArrayList<String>() {
        {
            add("APP");
            add("CONTROL");
            add("TV");
            add("VIDEO");
            add("CHAT");
        }
    };

    public static void main(String[] args) {
        SparkConf sc = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_test0");

        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> inputFile = jsc.textFile("hdfs://cloud" + args[0]);

        // into database
        JavaRDD<Document> macDoc = inputFile
                .map(JSONObject::parseObject)
                .filter(record->{
                    String domain = record.getString("return_domain");
                    return StringUtil.isNotEmpty(domain) &&
                            DOMAIN.contains(domain);
                })
                .mapToPair(record -> {
                    JSONObject keyJson = new JSONObject();
                    keyJson.put("query_text", record.getString("query_text"));
                    keyJson.put("domain", record.getString("return_domain"));
                    keyJson.put("intent", record.getString("return_intent"));
                    keyJson.put("source_flag", record.getInteger("source_flag"));
                    keyJson.put("semantic", record.getJSONObject("return_semantic"));
                    return new Tuple2<>(keyJson.toString(), 1);
                })
                .reduceByKey(Integer::sum)
                .map(record -> {
                    JSONObject keyJson = JSONObject.parseObject(record._1);
                    return new Document()
                            .append("query_text", keyJson.getString("query_text"))
                            .append("domain", keyJson.getString("domain"))
                            .append("intent", keyJson.getString("intent"))
                            .append("source_flag", keyJson.getInteger("source_flag"))
                            .append("semantic", keyJson.getJSONObject("semantic"))
                            .append("count", record._2);
                });

        MongoSpark.save(macDoc);

        jsc.close();
    }
}
