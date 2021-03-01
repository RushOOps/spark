package spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Durations, StreamingContext}

object StreamingKafkaProd {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-kafka-test")
    val streamingContext = new StreamingContext(conf, Durations.seconds(20))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.188.0.95:9092,10.188.0.96:9092,10.188.0.97:9092,10.188.0.98:9092,10.188.0.99:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "errorSuggest-queryCount",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("DS.Input.All.CSemantic")
    val batch = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // 只取值，读出来的key是null。本地测试经常出不来结果应该是网络原因。
    batch.map(record => record.value).print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
