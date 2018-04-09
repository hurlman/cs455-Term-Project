package cs455.spark.wordcount
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._

//This is a temprary demo content. Will deprecate it as new stuff gets added
class WordCount() 
{
   def Execute(mode:String, inputfile:String, outputpath:String) 
   {
      val conf = new SparkConf().setAppName("hw4-assignment").setMaster(mode);
      val sc = new SparkContext(conf);
      val input = sc.textFile(inputfile);
      val words = input.flatMap(line => line.split(" "));
      val counts = words.map( word => (word, 1)).reduceByKey{case(x,y) => x+y };
      counts.saveAsTextFile(outputpath);
   }
}
