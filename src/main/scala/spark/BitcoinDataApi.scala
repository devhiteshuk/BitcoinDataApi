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
      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }
}
