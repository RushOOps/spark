package spark;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

public class ContentMerge {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .config("spark.mongodb.input.uri", "mongodb://10.66.188.17/semantic.semantic_content_201909~202008")
                .config("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic.semantic_content_201909~202008_processed")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        JavaRDD<Document> input = MongoSpark.load(jsc);

        JavaRDD<Document> result = input
                .mapToPair(record -> new Tuple2<>(record.getString("query_text"), record))
                .combineByKey(
                        record -> record,
                        (record, newRecord) -> {
                            record.put("count", record.getInteger("count")+newRecord.getInteger("count"));
                            return record;
                        },
                        (record, newRecord) -> {
                            record.put("count", record.getInteger("count")+newRecord.getInteger("count"));
                            return record;
                        }
                )
                .map(record -> record._2);


        MongoSpark.save(result);

        jsc.stop();
    }
}
