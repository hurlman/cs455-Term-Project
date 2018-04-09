package cs455.spark.startup
import cs455.spark.wordcount.WordCount

//Main program entry.
object StartUp 
{
  def main(args: Array[String]): Unit = 
  {
     println("Word Count program");
     new WordCount().Execute(args(0), args(1),args(2));
  }
}
