package spark;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Iterables;
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
 * 924上线领域，每15天导一次
 */

public class OnlineDomain {

    private static final List<String> DOMAIN = new ArrayList<String>(){
        {
            add("MUSIC");
            add("POEM");
            add("JOKE");
            add("WorldRecords");
            add("IDIOM");
            add("REMINDER");
            add("STORY");
            add("PhoneCall");
            add("FOOD");
            add("WEATHER");
        }
    };

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_924");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> input = jsc.textFile("hdfs://hadoop1:9000"+args[0]);

        JavaRDD<Document> result = input
                .map(JSONObject::parseObject)
                .filter(record -> {
                    String domain = record.getString("return_domain");
                    return StringUtil.isNotEmpty(domain) &&
                            DOMAIN.contains(domain) &&
                            StringUtil.isNotEmpty("query_text");
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
