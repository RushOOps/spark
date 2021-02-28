package spark.rdd

import org.apache.spark.sql.SparkSession

case class Person(id: Long, name: String, age: Long)

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    import spark.implicits._

    val seqDS = Seq(Person(1,"anna",13)).toDS()
    seqDS.show()

    val primitiveDS = Seq(1,2,3).toDS()
    primitiveDS.show()

    val peopleDS = spark.read.json("/Users/crash/Desktop/demo.json").as[Person]
    peopleDS.show()

  }

}
