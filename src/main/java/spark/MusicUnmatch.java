package spark;

import org.apache.spark.SparkConf;

public class MusicUnmatch {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("MusicSourceFlag")
                .set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_music_week");
//        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("BaikeUrl").set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic_data_raw.BaikeUrl");



    }

}
