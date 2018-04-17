package cs455.spark.employment
import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
import java.io._


class TotalEmploymentAnalyzer() extends java.io.Serializable 
{
   //Sm area case class 
   case class SmArea(area_code:Int, area:String)

   //Sm Series case class 
   case class SmSeries(series_id: String, state_code: Int, area_code: Int, 
                       supersector_code: Int, industry_code: Int, data_type_code: Int, 
                       seasonal: String, benchmark_year: Int, footnote_codes: String, 
                       begin_year: Int, begin_period: String, end_year: Int, end_period: String);

   //Sm Data case class 
   case class SmDataSeries(series_id: String, year: Int, period:String, value:Int, footnode_codes:String)

     
   //Find Top/Bottom N of total non farm employment number of all the available areas.
   def Execute(sc:SparkContext, input_path:String, output_path:String) 
   {
        //val conf = new SparkConf().setAppName("hw4-assignment").setMaster(mode);
        //val sc = new SparkContext(conf);

        // Now, we will merge this with the area code file and generate the csv.
        // smArea: org.apache.spark.rdd.RDD[String]
        val smArea=sc.textFile(input_path + "/sm.area.txt")

        // Remove the header. 
        val smAreaHeader=smArea.first()
        val smAreaContent=smArea.filter(row => row != smAreaHeader)

        // New RDD of [ area_code(int), name(string) ]
        // smAreaCode2Name: org.apache.spark.rdd.RDD[(Int, String)]
        val smAreaCode2Name=smAreaContent.map(_.split("\t")).map(sma => (sma(0).toInt, sma(1)))


        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        // Get All relevant employment series.
        // Load the series file. smSeries: org.apache.spark.rdd.RDD[String]
        val smSeries=sc.textFile(input_path + "/sm.series.txt")
        // Remove the header. smSeriesContent: org.apache.spark.rdd.RDD[String]
        val smSeriesHeader=smSeries.first()
        val smSeriesContent=smSeries.filter(row => row != smSeriesHeader)
        // Create a RDD of sm series. smSeriesRDD: org.apache.spark.rdd.RDD[SmSeries]
        val smSeriesRDD = smSeriesContent.map(_.split("\t")).map(x => SmSeries(x(0),x(1).toInt,x(2).toInt,x(3).toInt,x(4).toInt, x(5).toInt, x(6), x(7).toInt, x(8), x(9).toInt,  x(10), x(11).toInt, x(12) ))
        // Filter series data based on our need.
        val smNonFarmPayrollAreaSeries = smSeriesRDD.filter( ss => ss.industry_code == 0  && ss.supersector_code == 0 && ss.data_type_code == 1 && ss.area_code !=0  && ss.seasonal == "S" )
        // Generate a RDD of series_id(string) , state_code(integer) and area_code(integer) : 
        // smNonFarmPayrollSeries2Area: org.apache.spark.rdd.RDD[String, (Int, Int)]
        val smNonFarmPayrollSeries2Area=smNonFarmPayrollAreaSeries.map(ss=> (ss.series_id, (ss.state_code, ss.area_code)))

        // Go to the data file and collect the series specific data.
        // Load the data file. smDataSeries: org.apache.spark.rdd.RDD[String]
        val smDataSeries=sc.textFile(input_path + "/sm.data.1.AllData.txt")
        // Remove the header. 
        val smDataSeriesHeader=smDataSeries.first()
        val smDataSeriesValContent=smDataSeries.filter(row => row != smDataSeriesHeader)
        // Create RDD of sm series data. smSeriesRDD: org.apache.spark.rdd.RDD[SmDataSeries]
        val smSeriesDataRDD = smDataSeriesValContent.map(_.split("\t")).map(x => SmDataSeries(x(0),x(1).toInt,x(2), if (x(3).trim == "-")  0 else ( x(3).trim.toFloat * 1000).toInt, x(4)))

        val smSeriesDataRDD_2007_2017 = smSeriesDataRDD.filter( ss => ss.year >= 2007  && ss.year <= 2017 )
        // Generate a RDD of series_id(string), year(integer) and total employment(integer) : 
        // smNonFarmPayrollSeries2JobCount: org.apache.spark.rdd.RDD[(String, Int, Int)]
        val smNonFarmPayrollSeries2JobCount=smSeriesDataRDD_2007_2017.map( sds =>  (sds.series_id, (sds.year, sds.value)))

        // Now, Let's join smNonFarmPayrollSeries2Area and smNonFarmPayrollSeries2JobCount. This will give only relevant information we need.
        // Joined new RDD of [series_id(string), ( state_code(integer) and area_code(integer) , (year(integer) and total employment(integer)) )] 
        // nonFarmPayrollSeries2AreaAndJobCount: org.apache.spark.rdd.RDD[(String, ((Int, Int), (Int, Int)))] 
        val nonFarmPayrollSeries2AreaAndJobCount = smNonFarmPayrollSeries2Area.join(smNonFarmPayrollSeries2JobCount)


        // New RDD of [ (series_id(string), Year(Int)) , (area_code(integer), total employment(integer)) ]
        // yearWiseAreaAndJobCount: org.apache.spark.rdd.RDD[((String, Int), (Int, Int))]
        val yearWiseAreaAndJobCount = nonFarmPayrollSeries2AreaAndJobCount.map( joinedrdd => ((joinedrdd._1, joinedrdd._2._2._1), 
                                                                                (joinedrdd._2._1._2, joinedrdd._2._2._2)) );

        // Find cumulative now.   
        // Now count total employment :   (series_id(string), Year(Int)) , (area_code, total_count)
        // reducedNonFarmPayrollSeries2AreaAndJobCount: org.apache.spark.rdd.RDD[((Int), (Int, Float))]
        val reducedNonFarmPayrollSeries2AreaAndJobCount = yearWiseAreaAndJobCount.reduceByKey((x, y)=> (x._1, x._2 + y._2)).map( xx => (xx._1, (xx._2._1, (xx._2._2/12).toFloat )) )


        // Now count total employment :   (area(Int)) , (year, total_count)
        // reducedYearWiseAreaAndJobCount : org.apache.spark.rdd.RDD[(Int, (Int, Float))]
        val reducedYearWiseAreaAndJobCount = reducedNonFarmPayrollSeries2AreaAndJobCount.map( ywaajc => ( ywaajc._2._1, (ywaajc._1._2, ywaajc._2._2)));


        //area2yearWiseJobCount: org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Float)])]
        val area2yearWiseJobCount = reducedYearWiseAreaAndJobCount.groupByKey();

        //area2yearWiseJobCountGroupByList: org.apache.spark.rdd.RDD[(Int, List[(Int, Float)])]  
        val area2yearWiseJobCountGroupByList = area2yearWiseJobCount.map( xx => (xx._1 , xx._2.toList));

        // area2sortedyearWiseJobCount: org.apache.spark.rdd.RDD[(Int, List[(Int, Float)])]
        val area2sortedyearWiseJobCount = area2yearWiseJobCountGroupByList.map( xx => (xx._1 , xx._2.sortBy(_._1)));


        //////////////////////////////////////////////////////////////////////////////////////////////////////////////   
        //Now First found out locations with maximum number of employment.
        val sortedBottomNNonFarmPayrollSeries2AreaAndJobCount = area2sortedyearWiseJobCount.sortBy(_._2.last._2).take(5)
        val sortedTopNFarmPayrollSeries2AreaAndJobCount = area2sortedyearWiseJobCount.sortBy(-_._2.last._2).take(5)


        
        // Extract the area code and total job and make two small new RDDs of [area_code(integer), total_employment(integer)]  to join with area name.
        // sortedBottomNArea2JobCount: org.apache.spark.rdd.RDD[(Int, Int)]
        // sortedTopNArea2JobCount: org.apache.spark.rdd.RDD[(Int, Int)]
        val sortedBottomNArea2JobCount = sc.parallelize(sortedBottomNNonFarmPayrollSeries2AreaAndJobCount.map(xx => (xx._1, xx._2.last._2)))
        val sortedTopNArea2JobCount = sc.parallelize(sortedTopNFarmPayrollSeries2AreaAndJobCount.map(xx => (xx._1, xx._2.last._2)))


        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Generate two Pair RDDs :  RDDs of [area_code(integer), (total_employment(integer), area_name(string))]  
        // sortedBottomNAreaName2JobCount: org.apache.spark.rdd.RDD[(Int, (Int, String))]
        // sortedTopNAreaName2JobCount: org.apache.spark.rdd.RDD[(Int, (Int, String))]
        val sortedBottomNAreaName2JobCount=sortedBottomNArea2JobCount.join(smAreaCode2Name).map(xx => ( xx._2._2 + "(" + xx._1 + ")", xx._2._1) ).sortBy(_._2)
        val sortedTopNAreaName2JobCount=sortedTopNArea2JobCount.join(smAreaCode2Name).map(xx => (xx._2._2 + "(" + xx._1 + ")" , xx._2._1) ).sortBy(-_._2)

        //Finally save in text format
        sortedBottomNAreaName2JobCount.coalesce(1).saveAsTextFile(output_path + "/total_emp/cumulative_bottom_5_area")
        sortedTopNAreaName2JobCount.coalesce(1).saveAsTextFile(output_path + "/total_emp/cumulative_top_5_area")
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



         //Find yearly growth now..
        val bottomNArea2JobCount = sc.parallelize(sortedBottomNNonFarmPayrollSeries2AreaAndJobCount.map(xx => (xx._1, xx._2)))
        val topNArea2JobCount = sc.parallelize(sortedTopNFarmPayrollSeries2AreaAndJobCount.map(xx => (xx._1, xx._2)))

        val bottomNAreaName2JobCount = bottomNArea2JobCount.join(smAreaCode2Name).map(xx => ( xx._2._2 + "(" + xx._1 + ")", xx._2._1)) 
        val topNAreaName2JobCount = topNArea2JobCount.join(smAreaCode2Name).map(xx => ( xx._2._2 + "(" + xx._1 + ")", xx._2._1)) 
        bottomNAreaName2JobCount.coalesce(1).saveAsTextFile(output_path + "/total_emp/debug/bottomNArea");
        topNAreaName2JobCount.coalesce(1).saveAsTextFile(output_path + "/total_emp/debug/topNArea");

          
        //Find yearly growth now for bottom 5
        val bottomNArea2JobYearlyGrowth = bottomNAreaName2JobCount.map ( xx => 
                                                  (xx._1, xx._2.map( xy =>
                                                     (xy._1, 
                                                      if ( xx._2.indexOf(xy) == 0 ) 0
                                                      else ((xy._2 - xx._2.apply(xx._2.indexOf(xy) - 1)._2) * 100) / xx._2.apply(xx._2.indexOf(xy) - 1)._2)
                                                     )))
        val flatBottomNArea2YearlyJobGrowth= bottomNArea2JobYearlyGrowth.flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
        flatBottomNArea2YearlyJobGrowth.coalesce(1).saveAsTextFile(output_path + "/total_emp/yearly_growth_in_low_employment_area");


        // Find cumulative growth for bottom 5
        val bottomNArea2JobCummulativeGrowth = bottomNAreaName2JobCount.map ( xx =>
                                                                          (xx._1, xx._2.map( xy =>
                                                                          (xy._1, ((xy._2 - xx._2.head._2)*100)/xx._2.head._2))));
        val flatBottomNArea2CumulativeJobGrowth=bottomNArea2JobCummulativeGrowth.flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
        flatBottomNArea2CumulativeJobGrowth.coalesce(1).saveAsTextFile(output_path + "/total_emp/cumulative_growth_in_low_employment_area");


        // Find yearly growth for top 5
        val topNArea2JobYearlyGrowth = topNAreaName2JobCount.map ( xx =>
                                                  (xx._1, xx._2.map( xy =>
                                                     (xy._1,
                                                      if ( xx._2.indexOf(xy) == 0 ) 0
                                                      else ((xy._2 - xx._2.apply(xx._2.indexOf(xy) - 1)._2) * 100) / xx._2.apply(xx._2.indexOf(xy) - 1)._2)
                                                     )))
        val flatTopNArea2YearlyJobGrowth=topNArea2JobYearlyGrowth.flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
        flatTopNArea2YearlyJobGrowth.coalesce(1).saveAsTextFile(output_path + "/total_emp/yearly_growth_in_high_employment_area");


        // Find cumulative growth for bottom 5
        val topNArea2JobCummulativeGrowth = topNAreaName2JobCount.map ( xx =>
                                                                          (xx._1, xx._2.map( xy =>
                                                                          (xy._1, ((xy._2 - xx._2.head._2)*100)/xx._2.head._2))));
        val flatTopNArea2CumulativeJobGrowth=topNArea2JobCummulativeGrowth.flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
        flatTopNArea2CumulativeJobGrowth.coalesce(1).saveAsTextFile(output_path + "/total_emp/cumulative_growth_in_high_employment_area");

   }    
}


