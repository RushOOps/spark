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

public class LifeWord {

    private static final List<String> LIFE_WORD = new ArrayList<String>(){
        {
            add("洗衣服");
            add("洗漱");
            add("洗脸");
            add("起床");
            add("打扫");
            add("困了");
            add("有点困");
            add("刷牙");
            add("洗脚");
            add("睡觉");
            add("睡了");
            add("出门");
            add("回家");
            add("饿了");
            add("有点饿");
            add("渴了");
            add("冷了");
            add("有点冷");
            add("热了");
            add("有点热");
            add("洗澡");
            add("扫地");
            add("拖地");
            add("洗碗");
            add("收拾");
        }
    };

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_life_word");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> input = jsc.textFile("hdfs://hadoop1:9000" + args[0]);
        JavaRDD<Document> result = input
                .map(JSONObject::parseObject)
                .filter(record -> {
                    String queryText = record.getString("query_text");
                    if (StringUtil.isEmpty(queryText)) return false;
                    for(String word : LIFE_WORD){
                        if(queryText.contains(word)) return true;
                    }
                    return false;
                })
                .mapToPair(record -> {
                    JSONObject keyJson = new JSONObject();
                    keyJson.put("query_text", record.getString("query_text"));
                    keyJson.put("domain", record.getString("return_domain"));
                    return new Tuple2<>(keyJson, 1);
                })
                .reduceByKey(Integer::sum)
                .map(record -> {
                    Document doc = new Document();
                    doc.put("query_text", record._1.getString("query_text"));
                    doc.put("domain", record._1.getString("domain"));
                    doc.put("count", record._2);
                    return doc;
                });
        MongoSpark.save(result);

        jsc.close();
    }
}
