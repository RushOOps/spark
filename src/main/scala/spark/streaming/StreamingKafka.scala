package spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Durations, StreamingContext}


object StreamingKafka {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-kafka-test")
    val streamingContext = new StreamingContext(conf, Durations.seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.188.0.95:9092,10.188.0.96:9092,10.188.0.97:9092,10.188.0.98:9092,10.188.0.99:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("DS.Input.All.CSemantic")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value)).print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
