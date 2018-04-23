package cs455.spark.employment
import org.apache.spark.SparkContext
import cs455.spark.util.Util._
import java.io._
import cs455.spark.commmon.ConstDefs._

///////////////////////////////////////////////////////////////////////////////////////
// This class finds out the median hourly pay
//////////////////////////////////////////////////////////////////////////////////////
class MedianHourlyPayAnalyzer() extends java.io.Serializable 
{
   //////////////////////////////////////////////////////////////////////////////////////////  
   // sc: SparkContext
   // input_path: Input path of the employment dataset.
   // output_path: Output path of the results
   /////////////////////////////////////////////////////////////////////////////////////////////////
   def Execute(sc:SparkContext, input_path:String, output_path:String):Unit =
   {
        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        //                             Get All the average hourly series.
        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        // Load the series file. smSeries: org.apache.spark.rdd.RDD[String]
        val smSeries=sc.textFile(input_path + "/sm.series.txt")
        // Remove the header. smSeriesContent: org.apache.spark.rdd.RDD[String]
        val smSeriesHeader=smSeries.first()

        // Create a RDD of series_id(string) , state_code(integer) and area_code(integer) : 
        val smFilteredSeries=smSeries.filter(row => row != smSeriesHeader)
                                    .map(_.split("\t"))
                                    .map(x => SmSeries(x(0),x(1).toInt,x(2).toInt,x(3).toInt,x(4).toInt, x(5).toInt, x(6), x(7).toInt, x(8), x(9).toInt,  x(10), x(11).toInt, x(12) ))
                                    .filter( ss => ss.industry_code > 0  && ss.supersector_code > 0 && ss.data_type_code == 3 && ss.area_code !=0  && ss.seasonal == "U" )
                                    .map(ss=> (ss.series_id, (ss.state_code, ss.area_code)))


        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        //                             Get the data series and join
        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        // Go to the data file and collect the series specific data.
        // Load the data file. smDataSeries: org.apache.spark.rdd.RDD[String]
        val smDataSeries=sc.textFile(input_path + "/sm.data.1.AllData.txt")
        // Remove the header. 
        // Create RDD of sm series data. smSeriesRDD: org.apache.spark.rdd.RDD[SmDataSeries]
        val series2YearAndAvgHourlyIncome=smDataSeries.filter(row => row.contains("M13") )
                                                      .map(_.split("\t")).map(x => SmDataSeries(x(0),x(1).toInt,x(2), if (x(3).trim == "-")  0 else (x(3).trim.toFloat).toInt, x(4)))
                                                      .filter( ss => ss.year >= START_YEAR  && ss.year <= END_YEAR )
                                                      .map( sds =>  (sds.series_id, (sds.year, sds.value)))


        // Now, Let's join series2YearAndAvgHourlyIncome and smAvgHourlyIncomeSeries2Area. This will give us the only relevant information we need.
        val yearWiseAreaAndMedianHourlyIncome = series2YearAndAvgHourlyIncome.join(smFilteredSeries)
                                                                             .map(joinedrdd => ((joinedrdd._2._2._2, joinedrdd._2._1._1), (joinedrdd._2._1._2)))
                                                                             .groupByKey();


        // Find median hourly income :   (area, year, total_count)
        // Final output : org.apache.spark.rdd.RDD[(Int, Int, Float))]
        yearWiseAreaAndMedianHourlyIncome.map( ywaajc => ((ywaajc._1._1, ywaajc._1._2),  median(ywaajc._2.toList )))
                                         .sortByKey()
                                         .coalesce(1)
                                         .saveAsTextFile( output_path + MEDIAN_HOURLY_PAY_OUTPUT_DIR_NAME);
   }    
}


