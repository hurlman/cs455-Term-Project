package cs455.spark.startup
import cs455.spark.wordcount.WordCount
import cs455.spark.employment.TotalEmploymentAnalyzer
import housing.HousingPriceAnalyzer
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import population.PopulationAnalyzer

//Main program entry.
object StartUp 
{
  def main(args: Array[String]): Unit = 
  {
    val ss = SparkSession.builder()
      .master(args(0))
      .appName("hw4-assignment")
      .getOrCreate()
     val sc = ss.sparkContext

     //println("Word Count program");
    // new TotalEmploymentAnalyzer().Execute(sc, (args(1) + "/employment_data"), args(2))
    new HousingPriceAnalyzer().Execute(ss, (args(1) + "/housing_data"), args(2))
    new PopulationAnalyzer().Execute(ss, (args(1) + "/population_data"), args(2))
  }
}
