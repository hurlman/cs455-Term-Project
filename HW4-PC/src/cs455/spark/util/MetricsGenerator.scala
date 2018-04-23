package cs455.spark.util
import org.apache.spark.SparkContext
import cs455.spark.commmon.ConstDefs._
import java.io._
import org.apache.spark.sql._
import org.apache.hadoop.fs._;

class MetricsGenerator() extends java.io.Serializable 
{
   def Execute(spark: SparkSession, input_path:String, output_path:String):Unit =
   {
        val sc = spark.sparkContext
        import spark.implicits._;
        val hsC=sc.textFile(input_path + HOUSING_STAT_OUTPUT_DIR_NAME + "/part-00000")
        val hsR=hsC.map{ line =>
                val x : Array[String] = line.replace("(", "").replace(")","").replace("Vector","").split(",")
                val y = x.map{ a => a.trim.toDouble }
                y(0).toInt -> List((2010, y(1)), (2011, y(2)), (2012, y(3)), (2013, y(4)) ,(2014, y(5)),(2015, y(6)),(2016, y(7)),(2017, y(8)))
                }.flatMapValues(xx=>xx).map(xx=> ((xx._1, xx._2._1), xx._2._2))

        val poC=sc.textFile(input_path + POPULATION_STAT_OUTPUT_DIR_NAME + "/part-00000")
        val poR=poC.map{ line =>
                val x : Array[String] = line.replace("(", "").replace(")","").replace("Vector","").split(",")
                val y = x.map{ a => a.trim.toDouble }
                y(0).toInt -> List((2010, y(1)), (2011, y(2)), (2012, y(3)), (2013, y(4)) ,(2014, y(5)),(2015, y(6)),(2016, y(7)),(2017, y(8)))
                }.flatMapValues(xx=>xx).map(xx=> ((xx._1, xx._2._1), xx._2._2))

        val eoC=sc.textFile(input_path + TOTAL_EMPLOYMENT_STAT_OUTPUT_DIR_NAME + "/part-00000")
        val eoR=eoC.map{ line =>
                val x : Array[String] = line.replace("(", "").replace(")","").replace("List","").split(",")
                val y = x.map{ a => a.trim.toDouble }
                y(0).toInt -> List((2010, y(1)), (2011, y(2)), (2012, y(3)), (2013, y(4)) ,(2014, y(5)),(2015, y(6)),(2016, y(7)),(2017, y(8)))
                }.flatMapValues(xx=>xx).map(xx=> ((xx._1, xx._2._1), xx._2._2))

        val ehoC=sc.textFile(input_path + MEDIAN_HOURLY_PAY_OUTPUT_DIR_NAME + "/part-00000")
        val ehoR=ehoC.map{ line =>
                val x : Array[String] = line.replace("(", "").replace(")","").replace("List","").split(",")
                val y = x.map{ a => a.trim.toDouble }
                (y(0).toInt, y(1).toInt) -> y(2).toDouble
                }

        val joineRDD=hsR.join(eoR).join(poR).join(ehoR).sortByKey().map(xx=> (xx._1._1, xx._1._2,  xx._2._1._1._1, xx._2._1._1._2, xx._2._1._2, xx._2._2));
        joineRDD.toDF("metro_code", "year", "median_single_home", "total_employment","total_population","median_hourly_income")
                .coalesce(1).write.option("header", "true").format("csv").save(output_path + ALL_METRICS_OUTPUT_DIR_NAME);

        val fs = FileSystem.get(sc.hadoopConfiguration);
        val file = fs.globStatus(new Path( output_path + ALL_METRICS_OUTPUT_DIR_NAME + "part*"))(0).getPath().getName();
        fs.rename(new Path(output_path + ALL_METRICS_OUTPUT_DIR_NAME + file), new Path(output_path + ALL_METRICS_OUTPUT_DIR_NAME + ALL_METRICS_OUTPUT_FILE_NAME));
   }    
}


