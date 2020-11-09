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

public class MongoExtract {

    private static final List<String> DOMAIN_FILTER = new ArrayList<String>(){
        {
            add("JOKE");
            add("WorldRecords");
            add("HTWHYS");
        }
    };

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("TencentExtract").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_tencent");
//        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("BaikeUrl").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic_data_raw.BaikeUrl");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = jsc.textFile("hdfs://cloud" + args[0]);
//        JavaRDD<String> inputFile = jsc.textFile("/Users/crash/Desktop/test");
        JavaRDD<Document> doc = inputFile
                .map(JSONObject::parseObject)
                .filter(input -> {
                    String domain = input.getString("domain");
                    JSONObject content = input.getJSONObject("content");
                    return StringUtil.isNotEmpty(domain)
                            && DOMAIN_FILTER.contains(domain)
                            && StringUtil.isNotEmpty(input.getString("queryText"))
                            && content != null
                            && !content.isEmpty();
                })
                .mapToPair(input -> {
                    JSONObject json = new JSONObject();
                    json.put("domain", input.getString("domain"));
                    json.put("queryText", input.getString("queryText"));
                    json.put("content", input.getJSONObject("content"));
                    return new Tuple2<>(json.toString(), 1);
                })
                .reduceByKey(Integer::sum)
                .map(input -> {
                    JSONObject json = JSONObject.parseObject(input._1);
                    return new Document()
                            .append("domain", json.getString("domain"))
                            .append("query_text", json.getString("queryText"))
                            .append("content", json.getJSONObject("content"))
                            .append("count", input._2);
                });
        MongoSpark.save(doc);
    }
}
