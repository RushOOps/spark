package spark;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class TencentVsChanghong {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> input = jsc.textFile("hdfs://hadoop1:9000" + args[0]);

        LongAccumulator tencent = jsc.sc().longAccumulator("Tencent");
        LongAccumulator changhong = jsc.sc().longAccumulator("Changhong");
        LongAccumulator emptySourceFlag = jsc.sc().longAccumulator("EmptySourceFlag");

        input.map(record -> {
            JSONObject recordJson = JSONObject.parseObject(record);
            Integer sourceFlag = recordJson.getInteger("source_flag");
            if (sourceFlag == null) {
                emptySourceFlag.add(1);
                return null;
            }
            if(sourceFlag == 5) tencent.add(1);
            if(sourceFlag != 5 && sourceFlag != 15) changhong.add(1);
            return null;
        }).count();

        jsc.stop();
    }
}
