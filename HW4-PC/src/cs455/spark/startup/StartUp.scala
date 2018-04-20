package cs455.spark.startup

import cs455.spark.employment._
import cs455.spark.housing._
import cs455.spark.population._
import org.apache.spark.sql._

//Main program entry.
object StartUp {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master(args(0))
      .appName("hw4-assignment")
      .getOrCreate()
     val sc = ss.sparkContext

     //println("Word Count program");
     new TotalEmploymentAnalyzer().Execute(sc, (args(1) + "/employment_data"), args(2), "null");
     new TotalEmploymentAnalyzer().Execute(sc, (args(1) + "/employment_data"), args(2), args(1) + "/" + "selective_area.txt");
     new HousingPriceAnalyzer().Execute(ss, args(1) + "/housing_data", args(2))
     new PopulationAnalyzer().Execute(ss, args(1) + "/population_data", args(2))
  }
}
