package cs455.spark.startup

import cs455.spark.employment._
import cs455.spark.housing._
import cs455.spark.population._
import cs455.spark.model._
import cs455.spark.commmon.ConstDefs._
import cs455.spark.util._
import org.apache.spark.sql._

//Main program entry.
object StartUp {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master(args(0))
      .appName("hw4-assignment")
      .getOrCreate()
     val sc = ss.sparkContext

    val inputFile = args.length > 3

    // Analyze total employment.
    new TotalEmploymentAnalyzer().Execute(sc, args(1) + EMPLOYMENT_INPUT_DATA_DIR_NAME, args(2), inputFile)

    // Analyze housing price.
    new HousingPriceAnalyzer().Execute(ss, args(1) + HOUSING_INPUT_DATA_DIR_NAME, args(2), inputFile)

    // Analyze population growth.
    new PopulationAnalyzer().Execute(ss, args(1) + POPULATION_INPUT_DATA_DIR_NAME, args(2), inputFile)

    // Analyze median hourly pay.
    new MedianHourlyPayAnalyzer().Execute(sc, args(1) + EMPLOYMENT_INPUT_DATA_DIR_NAME, args(2))

    // Data metric generator
    new MetricsGenerator().Execute(ss, args(2), args(2))

    // Execute linear regression tasks.
     new LinearRegresser().Execute(ss, args(2), args(2))
  }

}
