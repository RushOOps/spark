package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.MongoClient
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object Universal {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic.semantic_children_mac")
    val sc = new SparkContext(conf)

    val child = Array("孩子", "小孩", "儿童", "小朋友")
    val baby = Array("宝宝", "婴儿")
    val childBC = sc.broadcast(child)
    val babyBC = sc.broadcast(baby)

    val domains = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+"/domains.txt").collect.toList
    val domainsBC = sc.broadcast(domains)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    // --conf spark.executor.memory=2G
    val cache = input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) &&
          domainsBC.value.contains(domain)
      })
      .mapPartitions(partition => {
        val client = new MongoClient("10.66.188.17", 27017)
        val collection = client.getDatabase("SemanticLog").getCollection("mac_label")
        val returnArr = ArrayBuffer.empty[JSONObject]
        partition.foreach(record => {
          if(collection.countDocuments(new Document("mac", record.getString("query_mac"))) == 0){
            returnArr.append(record)
          }
        })
        client.close()
        returnArr.iterator
      })
      .cache

    cache
      .map(record => (record.getString("return_domain"), 1))
      .reduceByKey(_+_)
      .map(record => record._1+" "+record._2)
      .saveAsTextFile("hdfs://hadoop1:9000/output")

    val result = cache
      .flatMap(record => {
        val queryText = record.getString("query_text")
        val returnArr = ArrayBuffer.empty[((String,String,String),Int)]
        for(c <- childBC.value){
          if(queryText.contains(c)) {
            returnArr.append((("孩子",record.getString("query_mac"),record.getString("return_domain")),1))
          }
        }
        if(returnArr.isEmpty){
          for(c <- babyBC.value){
            if(queryText.contains(c)) {
              returnArr.append((("婴儿",record.getString("query_mac"),record.getString("return_domain")),1))
            }
          }
        }
        returnArr
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("type", record._1._1)
        .append("domain", record._1._3)
        .append("mac", record._1._2)
        .append("count", record._2)
      )

    MongoSpark.save(result)

    sc.stop()
  }

}
