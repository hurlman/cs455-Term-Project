package cs455.spark.startup
import cs455.spark.wordcount.WordCount
import cs455.spark.employment.TotalEmploymentAnalyzer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//Main program entry.
object StartUp 
{
  def main(args: Array[String]): Unit = 
  {
     val conf = new SparkConf().setAppName("hw4-assignment").setMaster(args(0));
     val sc = new SparkContext(conf);

     //println("Word Count program");
     new TotalEmploymentAnalyzer().Execute(sc, (args(1) + "/employment_data"), args(2));
  }
}
