package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._

object BitcoinDataApi {
  def main(args: Array[String]): Unit = {
    //Start spark session
    val spark = SparkSession.builder().appName("BitcoinDataApi").master("local[*]").getOrCreate()

    while (true) {
      import spark.implicits._
      //API details loading
      val apiUrl = "https://api.tfl.gov.uk/Line/victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"
      val response = get(apiUrl, headers = headers)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      println(s"This is data frame text --> ${dfFromText}")

      val messageDF = dfFromText.select($"id", $"stationName", $"lineName", $"towards",
        $"expectedArrival",$"vehicleId",$"platformName",$"direction",$"destinationName",
        $"timestamp",$"timeToStation", $"currentLocation",$"timeToLive")

      val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "arrivaldata"

      messageDF.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()
      println("message is loaded to kafka topic")

      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }
}
