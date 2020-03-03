package klpo
import org.apache.spark.sql.SparkSession

object SparkWordCount extends App {

  val spark = SparkSession.builder

 .master("yarn")
//    .appName("Sample Jobs")
//    .config("spark-yarn.jars","hdfs://54.183.183.100:8020/")
//    .getOrCreate()
//
//    val stocsDF=spark.read
//
//  .option("header","true")
//      .csv("/user/")
//
//
  //stocsDF.show()
    .master("local[*]")
    .appName("Spark Word 45 Count 67")
    .getOrCreate()

  val lines = spark.sparkContext.parallelize(
    Seq("Spark Intellij Idea Scala test one",
      "Spark Intellij Idea Scala test two",
      "Spark Intellij Idea Scala test three"))

  val counts = lines
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)
}
