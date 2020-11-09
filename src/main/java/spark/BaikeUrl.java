package spark;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import util.StringUtil;

public class BaikeUrl {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("BaikeUrl").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic_data_raw.BaikeUrl");
//        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("BaikeUrl").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic_data_raw.BaikeUrl");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = jsc.textFile("hdfs://cloud" + args[0]);
//        JavaRDD<String> inputFile = jsc.textFile("/Users/crash/Desktop/test");

        JavaRDD<Document> doc = inputFile
                .map(JSONObject::parseObject)
                .filter(r -> {
                    if(StringUtil.strEquals(r.getString("domain"), "BAIKE")){
                        JSONObject contentJson = r.getJSONObject("content");
                        if (contentJson == null) return false;
                        return StringUtil.isNotEmpty(contentJson.getString("sPageUrl"));
                    }
                    return false;
                })
                .map(r -> r.getJSONObject("content").getString("sPageUrl"))
                .distinct()
                .map(r -> new Document("sPageUrl", r));

        MongoSpark.save(doc);
    }
}
