package spark;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;
import util.StringUtil;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DomainWord {

    private static final OkHttpClient httpClient = new OkHttpClient().newBuilder()
            .connectTimeout(5, TimeUnit.MINUTES)
            .readTimeout(5, TimeUnit.MINUTES)
            .build();

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN);
        SparkConf sc = new SparkConf().setAppName("DomainWord").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_domain_word");
//        SparkConf sc = new SparkConf().setAppName("DomainWord").setMaster("local").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_domain_word");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> inputFile = jsc.textFile("hdfs://cloud" + args[0]);
//        JavaRDD<String> inputFile = jsc.textFile("/Users/crash/Downloads/test.txt");

        JavaRDD<Document> documents = inputFile.filter(text -> {
            JSONObject testJson = JSONObject.parseObject(text);
            String queryText = testJson.getString("query_text");
            String domain = testJson.getString("return_domain");
            return !StringUtil.isEmpty(domain) && !StringUtil.isEmpty(queryText);
        }).mapToPair(text -> {
            JSONObject textJson = JSONObject.parseObject(text);
            String queryText = textJson.getString("query_text");
            String domain = textJson.getString("return_domain");
            return new Tuple2<>(domain+"|"+queryText, 1);
        }).reduceByKey(Integer::sum).flatMapToPair(content -> {

            String[] keys = content._1.split("\\|");
            List<Tuple2<String, Integer>> list= new ArrayList<>();
            Request request = new Request.Builder()
                    .url("http://hwtest.semantic-nlp.chiq-cloud.com/NlpBasicTasks?userFlag=0&words="+URLEncoder.encode(keys[1], "UTF-8"))
                    .build();
            Response response = httpClient.newCall(request).execute();
            if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
            if(response.body() == null){
                list.add(new Tuple2<>("BODYNULL|"+keys[1], content._2));
                return list.iterator();
            }
            JSONObject fenciJson = JSONObject.parseObject(response.body().string());
            JSONArray words = null;

            if(fenciJson.containsKey("words")){
                words = fenciJson.getJSONArray("words");
            }

            if(words == null || words.size() == 0){
                list.add(new Tuple2<>("WORDSERROR|"+keys[1], content._2));
                return list.iterator();
            }

            for(int i = 0; i < words.size(); i++){
                JSONObject word = words.getJSONObject(i);
                list.add(new Tuple2<>(keys[0]+"|"+word.getString("name"), content._2));
            }

            return list.iterator();
        }).reduceByKey(Integer::sum).mapToPair(result -> {
            String[] keys = result._1.split("\\|");
            JSONObject value = new JSONObject();
            value.put(keys[0], result._2);
            return new Tuple2<>(keys[1], value);
        }).groupByKey().map(result -> {
            JSONObject total = new JSONObject();
            for(JSONObject j : result._2){
                total.putAll(j);
            }
            return new Document()
                    .append("word", result._1)
                    .append("count", total);
        });

        // 存入mongo
        MongoSpark.save(documents);
    }

}
