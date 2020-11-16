package spark;

//val conf = new SparkConf().set()
//        val sc = new SparkContext(conf)
//
//        val input = sc.textFile()
//
//        val result = input.map(record => JSON.parseObject(record))
//        .filter(record => {
//        val domain = record.getString("return_domain")
//        val semantic = record.getJSONObject("return_semantic")
//        StringUtil.isNotEmpty(domain) &&
//        domain.equals("MUSIC") &&
//        semantic != null &&
//        semantic.size() != 0
//        })
//        .flatMap(record => {
//        val semantic = record.getJSONObject("return_semantic").entrySet().iterator()
//        val returnSeq = new ArrayBuffer[(String, Int)]()
//        while(semantic.hasNext){
//        val keyValue = semantic.next()
//        returnSeq.append((keyValue.getKey+"@@@"+keyValue.getValue, 1))
//        }
//        returnSeq
//        })
//        .reduceByKey(_+_)
//        .map(record => {
//        val keyValue = record._1.split("@@@")
//        new Document()
//        .append("property", keyValue(0))
//        .append("value", keyValue(1))
//        .append("count", record._2)
//        })

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Iterables;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;
import util.StringUtil;

public class Music {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_music_ktv");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> input = jsc.textFile("hdfs://hadoop1:9000"+args[0]);

        JavaRDD<Document> result = input
                .map(JSONObject::parseObject)
                .filter(record -> {
                    String domain = record.getString("return_domain");
                    String intent = record.getString("return_intent");
                    return StringUtil.isNotEmpty(domain) &&
                            StringUtil.isNotEmpty(intent) &&
                            domain.equals("MUSIC") &&
                            intent.equals("KTV");
                })
                .mapToPair(record -> {
                    JSONObject value = new JSONObject();
                    value.put("domain", record.getString("return_domain"));
                    value.put("intent", record.getString("return_intent"));
                    value.put("semantic", record.getJSONObject("return_semantic"));
                    return new Tuple2<>(record.getString("query_text"), value.toString());
                })
                .groupByKey()
                .map(record -> {
                    int total = Iterables.size(record._2);
                    JSONObject value = JSONObject.parseObject(record._2.iterator().next());
                    Document doc = new Document();
                    doc.put("query_text", record._1);
                    doc.put("domain", value.getString("domain"));
                    doc.put("intent", value.getString("intent"));
                    doc.put("semantic", value.getJSONObject("semantic"));
                    doc.put("count", total);
                    return doc;
                });

        MongoSpark.save(result);

        jsc.stop();
    }
}
