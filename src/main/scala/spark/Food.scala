package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object Food {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_food_09")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000"+args(0))

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        val semantic = record.getJSONObject("return_semantic")
        StringUtil.isNotEmpty(domain) &&
          domain.equals("FOOD") &&
          semantic != null &&
          StringUtil.isNotEmpty(semantic.getString("food"))
      })
      .map(record => {
        val semantic = record.getJSONObject("return_semantic")
        var food = semantic.getString("food")
        val brand = semantic.getString("brand")
        if(StringUtil.isNotEmpty(brand)){
          food = brand.concat(food)
        }
        (food, 1)
      })
      .reduceByKey(_+_)
      .map(record => {
        new Document()
          .append("food", record._1)
          .append("count", record._2)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

