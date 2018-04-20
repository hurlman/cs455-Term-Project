package cs455.spark.employment
import org.apache.spark.SparkContext
import java.io._

///////////////////////////////////////////////////////////////////////////////////////
// This class finds out the following information from the employment data
// 1. Cummulative(average) Top N employment locations(metro area).
// 2. Cummulative(average) Bottom N employment locations(metro area).
// 3. Yearly growth in high employment area.
// 4. Yearly growth in low emplyment area.
// 5. Cumulative growth on every year from a reference year in high employment area.
// 6. Cumulative growth on every year from a reference year in low employment area.
//////////////////////////////////////////////////////////////////////////////////////
class TotalEmploymentAnalyzer() extends java.io.Serializable 
{
   val NUM_ELEMENT_IN_TOP_OR_BOTTOM = 5;
   val START_YEAR = 2010;
   val END_YEAR = 2017;
   //exclude puertorico list.
   val Exclude_PR_List = List(38660,32420,25020,11640,10380,41980,41900)


   //Sm area case class 
   case class SmArea(area_code:Int, area:String)

   //Sm Series case class 
   case class SmSeries(series_id: String, state_code: Int, area_code: Int, 
                       supersector_code: Int, industry_code: Int, data_type_code: Int, 
                       seasonal: String, benchmark_year: Int, footnote_codes: String, 
                       begin_year: Int, begin_period: String, end_year: Int, end_period: String);

   //Sm Data case class 
   case class SmDataSeries(series_id: String, year: Int, period:String, value:Int, footnode_codes:String)

   //////////////////////////////////////////////////////////////////////////////////////////  
   // 1. Cummulative(average) Top N employment locations(metro area).
   // 2. Cummulative(average) Bottom N employment locations(metro area).
   // 3. Yearly growth in high employment area.
   // 4. Yearly growth in low emplyment area.
   // 5. Cumulative growth on every year from a reference year in high employment area.
   // 6. Cumulative growth on every year from a reference year in low employment area.
   //////////////////////////////////////////////////////////////////////////////////////////
   // sc: SparkContext
   // input_path: Input path of the employment dataset.
   // output_path: Output path of the results
   /////////////////////////////////////////////////////////////////////////////////////////////////
   def Execute(sc:SparkContext, input_path:String, output_path:String, inputFile: Boolean):Unit =
   {
        /////////////////////////////////////////////////////////////////////////////////////
        // Get the Area Code 2 Mapping Ready. We will use join instead of manual look up
        ////////////////////////////////////////////////////////////////////////////////////
        // Read SM area text file. smArea: org.apache.spark.rdd.RDD[String]
        val smArea=sc.textFile(input_path + "/sm.area.txt")
        // Remove the header. 
        val smAreaHeader=smArea.first()
        val smAreaContent=smArea.filter(row => row != smAreaHeader)
        // Extract the area code and name by creating using new RDD of [ area_code(int), name(string) ]
        // smAreaCode2Name: org.apache.spark.rdd.RDD[(Int, String)]
        val smAreaCode2Name=smAreaContent.map(_.split("\t")).map(sma => (sma(0).toInt, sma(1)))


        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        //                             Get All relevant employment series.
        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        // Load the series file. smSeries: org.apache.spark.rdd.RDD[String]
        val smSeries=sc.textFile(input_path + "/sm.series.txt")
        // Remove the header. smSeriesContent: org.apache.spark.rdd.RDD[String]
        val smSeriesHeader=smSeries.first()
        val smSeriesContent=smSeries.filter(row => row != smSeriesHeader)
        // Create a RDD of sm series. smSeriesRDD: org.apache.spark.rdd.RDD[SmSeries]
        val smSeriesRDD = smSeriesContent.map(_.split("\t")).map(x => SmSeries(x(0),x(1).toInt,x(2).toInt,x(3).toInt,x(4).toInt, x(5).toInt, x(6), x(7).toInt, x(8), x(9).toInt,  x(10), x(11).toInt, x(12) ))
        // Filter series data based on our need. We are only interested in non farm payroll data (industry code == 0 and super sector == 0)
        // for metro areas, and we will consider the adjusted data.
        val smNonFarmPayrollAreaSeries = smSeriesRDD.filter( ss => ss.industry_code == 0  && ss.supersector_code == 0 && ss.data_type_code == 1 && ss.area_code !=0  && ss.seasonal == "S" )
        // Generate a RDD of series_id(string) , state_code(integer) and area_code(integer) : 
        // smNonFarmPayrollSeries2Area: org.apache.spark.rdd.RDD[String, (Int, Int)]
        val smNonFarmPayrollSeries2Area=smNonFarmPayrollAreaSeries.map(ss=> (ss.series_id, (ss.state_code, ss.area_code))).filter(x => !Exclude_PR_List.contains(x._2._2))


        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        //                             Get All the data and process
        /////////////////////////////////////////////////////////////////////////////////////////////////// 
        // Go to the data file and collect the series specific data.
        // Load the data file. smDataSeries: org.apache.spark.rdd.RDD[String]
        val smDataSeries=sc.textFile(input_path + "/sm.data.1.AllData.txt")
        // Remove the header. 
        val smDataSeriesHeader=smDataSeries.first()
        val smDataSeriesValContent=smDataSeries.filter(row => row != smDataSeriesHeader)
        // Create RDD of sm series data. smSeriesRDD: org.apache.spark.rdd.RDD[SmDataSeries]
        val smSeriesDataRDD = smDataSeriesValContent.map(_.split("\t")).map(x => SmDataSeries(x(0),x(1).toInt,x(2), if (x(3).trim == "-")  0 else ( x(3).trim.toFloat * 1000).toInt, x(4)))

        val smSeriesDataRDDOfAPeriod = smSeriesDataRDD.filter( ss => ss.year >= START_YEAR  && ss.year <= END_YEAR )
        // Generate a RDD of series_id(string), year(integer) and total employment(integer) : 
        // smNonFarmPayrollSeries2JobCount: org.apache.spark.rdd.RDD[(String, Int, Int)]
        val smNonFarmPayrollSeries2JobCount=smSeriesDataRDDOfAPeriod.map( sds =>  (sds.series_id, (sds.year, sds.value)))

        // Now, Let's join smNonFarmPayrollSeries2Area and smNonFarmPayrollSeries2JobCount This will give us the only relevant information we need.
        // Joined new RDD of [series_id(string), ( state_code(integer) and area_code(integer) , (year(integer) and total employment(integer)) )] 
        // nonFarmPayrollSeries2AreaAndJobCount: org.apache.spark.rdd.RDD[(String, ((Int, Int), (Int, Int)))] 
        val nonFarmPayrollSeries2AreaAndJobCount = smNonFarmPayrollSeries2Area.join(smNonFarmPayrollSeries2JobCount)


        // Get rid of area code and make a composite key of series Id and year and map the values.
        // This will give us monthly employment information of an area over a time period. 
        // New RDD of [ (series_id(string), Year(Int)) , (area_code(integer), total employment(integer)) ]
        // yearWiseAreaAndJobCount: org.apache.spark.rdd.RDD[((String, Int), (Int, Int))]
        val yearWiseAreaAndJobCount = nonFarmPayrollSeries2AreaAndJobCount.map( joinedrdd => ((joinedrdd._1, joinedrdd._2._2._1), 
                                                                                (joinedrdd._2._1._2, joinedrdd._2._2._2)) );

        // We will reduce the monthly information to yearly and calculate average to find out average yearly employment
        // Now count total employment :   (series_id(string), Year(Int)) , (area_code, total_count)
        // reducedNonFarmPayrollSeries2AreaAndJobCount: org.apache.spark.rdd.RDD[((String, Int), (Int, Float))]
        val reducedNonFarmPayrollSeries2AreaAndJobCount = yearWiseAreaAndJobCount.reduceByKey((x, y)=> (x._1, x._2 + y._2)).map( xx => (xx._1, (xx._2._1, (xx._2._2/12).toFloat )) )


        // Let's remove the series information from the data and map the data to location and yearly information
        // Now count total employment :   (area(Int)) , (year, total_count)
        // reducedYearWiseAreaAndJobCount : org.apache.spark.rdd.RDD[(Int, (Int, Float))]
        val reducedYearWiseAreaAndJobCount = reducedNonFarmPayrollSeries2AreaAndJobCount.map( ywaajc => ( ywaajc._2._1, (ywaajc._1._2, ywaajc._2._2)));

        // Let's group this information area(key). Not reducing it yet.
        //area2yearWiseJobCount((area(Int)) , ((year1, total_count1), (year1, total_count2)..)): org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Float)])]
        val area2yearWiseJobCount = reducedYearWiseAreaAndJobCount.groupByKey();
        // area2yearWiseJobCountGroupByList: org.apache.spark.rdd.RDD[(Int, List[(Int, Float)])]  
        val area2yearWiseJobCountGroupByList = area2yearWiseJobCount.map( xx => (xx._1 , xx._2.toList));

        // Next we will compute cumulative growth. Sort by year first.
        // area2sortedyearWiseJobCount: org.apache.spark.rdd.RDD[(Int, List[(Int, Float)])]
        val area2sortedyearWiseJobCount = area2yearWiseJobCountGroupByList.map( xx => (xx._1 , xx._2.sortBy(_._1)));

        if ( inputFile )
        {
            val RelevantList=sc.textFile(input_path.replace("/employment_data", "/selective_area.txt")).collect();
            val specificMetroArea2JobCount = area2sortedyearWiseJobCount.filter(x=> RelevantList.contains(x._1.toString)).join(smAreaCode2Name)
                                                                                    .map(xx => ( xx._2._2 + "(" + xx._1 + ")", xx._2._1) )
            specificMetroArea2JobCount.map( xx => (xx._1, xx._2.map( xy => (xy._1, if ( xx._2.indexOf(xy) == 0 ) 0 else ((xy._2 - xx._2.apply(xx._2.indexOf(xy) - 1)._2) * 100) / xx._2.apply(xx._2.indexOf(xy) - 1)._2))))
                                      .flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
                                      .coalesce(1).saveAsTextFile(output_path + "/employment/yearly_growth_in_selective_area")

            specificMetroArea2JobCount.map ( xx =>  (xx._1, xx._2.map( xy => (xy._1, ((xy._2 - xx._2.head._2)*100)/xx._2.head._2))))
                                      .flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
                                      .coalesce(1).saveAsTextFile(output_path + "/employment/cumulative_growth_in_selective_area")

            area2sortedyearWiseJobCount.filter(x=> RelevantList.contains(x._1.toString)).join(smAreaCode2Name)
                                       .coalesce(1).saveAsTextFile(output_path + "/employment/total_in_selective_area")
        }
        else
        {
            val area2sortedyearWiseJobCountAndCumulativeGrowth  = area2sortedyearWiseJobCount.map(xx => (xx._1, xx._2, (xx._2.last._2 - xx._2.head._2)*100/xx._2.head._2))
            val bottomNNonFarmPayrollSeries2AreaAndJobCount = area2sortedyearWiseJobCountAndCumulativeGrowth.sortBy(_._3).take(NUM_ELEMENT_IN_TOP_OR_BOTTOM)
            val topNNonFarmPayrollSeries2AreaAndJobCount = area2sortedyearWiseJobCountAndCumulativeGrowth.sortBy(-_._3).take(NUM_ELEMENT_IN_TOP_OR_BOTTOM)
            
            
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //                                   Computing final results       
            //                  1. Cummulative(average) Top N employment locations(metro area).
            //                  2. Cummulative(average) Bottom N employment locations(metro area).
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //First found out locations with maximum number of employment.
            // Extract the area code and total job and make two small new RDDs of [area_code(integer), total_employment(integer)]  to join with area name.
            // sortedBottomNArea2JobCount: org.apache.spark.rdd.RDD[(Int, Int)],  sortedTopNArea2JobCount: org.apache.spark.rdd.RDD[(Int, Int)]
            val sortedBottomNArea2JobCount = sc.parallelize(bottomNNonFarmPayrollSeries2AreaAndJobCount.map(xx => (xx._1, xx._3)))
            val sortedTopNArea2JobCount = sc.parallelize(topNNonFarmPayrollSeries2AreaAndJobCount.map(xx => (xx._1, xx._3)))
            
            
            // Generate two Pair RDDs :  RDDs of [area_code(integer), (total_employment(integer), area_name(string))]  
            // sortedBottomNAreaName2JobCount: org.apache.spark.rdd.RDD[(Int, (Int, String))], 
            // sortedTopNAreaName2JobCount: org.apache.spark.rdd.RDD[(Int, (Int, String))]
            val sortedBottomNAreaName2JobCount=sortedBottomNArea2JobCount.join(smAreaCode2Name).map(xx => ( xx._2._2 + "(" + xx._1 + ")", xx._2._1) ).sortBy(_._2)
            val sortedTopNAreaName2JobCount=sortedTopNArea2JobCount.join(smAreaCode2Name).map(xx => (xx._2._2 + "(" + xx._1 + ")" , xx._2._1) ).sortBy(-_._2)
            
            //Finally save in text format
            sortedBottomNAreaName2JobCount.coalesce(1).saveAsTextFile(output_path + "/employment/cumulative_bottom_N_area")
            sortedTopNAreaName2JobCount.coalesce(1).saveAsTextFile(output_path + "/employment/cumulative_top_N_area")
            
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //                                   Computing final results       
            //                         3. Yearly growth in high employment area.
            //                         4. Yearly growth in low emplyment area.
            //              5. Cumulative growth on every year from a reference year in high employment area.
            //              6. Cumulative growth on every year from a reference year in low employment area.
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            // Create small RDDs for top and bottoms
            val sortedBottomNNonFarmPayrollSeries2AreaAndJobCount = sc.parallelize(bottomNNonFarmPayrollSeries2AreaAndJobCount.map(xx => (xx._1, xx._2))) ;
            val sortedTopNNonFarmPayrollSeries2AreaAndJobCount    = sc.parallelize(topNNonFarmPayrollSeries2AreaAndJobCount.map(xx => (xx._1, xx._2)));
            
            // Let's ensure that the data is sorted uearwise. This has almost no cost due to an RDD of Top/Bottom N elements. 
            val bottomNArea2JobCount = sortedBottomNNonFarmPayrollSeries2AreaAndJobCount.map( xx => (xx._1, xx._2.sortBy(_._1)))
            val topNArea2JobCount = sortedTopNNonFarmPayrollSeries2AreaAndJobCount.map( xx => (xx._1, xx._2.sortBy(_._1)))
            
            val bottomNAreaName2JobCount = bottomNArea2JobCount.join(smAreaCode2Name).map(xx => ( xx._2._2 + "(" + xx._1 + ")", xx._2._1)) 
            val topNAreaName2JobCount = topNArea2JobCount.join(smAreaCode2Name).map(xx => ( xx._2._2 + "(" + xx._1 + ")", xx._2._1)) 
            bottomNAreaName2JobCount.coalesce(1).saveAsTextFile(output_path + "/employment/debug/bottomNArea");
            topNAreaName2JobCount.coalesce(1).saveAsTextFile(output_path + "/employment/debug/topNArea");
            
              
            // Find yearly growth now for bottom N
            val bottomNArea2JobYearlyGrowth = bottomNAreaName2JobCount.map ( xx => 
                                                      (xx._1, xx._2.map( xy =>
                                                         (xy._1, 
                                                          if ( xx._2.indexOf(xy) == 0 ) 0
                                                          else ((xy._2 - xx._2.apply(xx._2.indexOf(xy) - 1)._2) * 100) / xx._2.apply(xx._2.indexOf(xy) - 1)._2)
                                                         )))
            val flatBottomNArea2YearlyJobGrowth= bottomNArea2JobYearlyGrowth.flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
            flatBottomNArea2YearlyJobGrowth.coalesce(1).saveAsTextFile(output_path + "/employment/yearly_growth_in_low_employment_area");
            
            
            // Find cumulative growth for bottom N
            val bottomNArea2JobCummulativeGrowth = bottomNAreaName2JobCount.map ( xx =>
                                                                              (xx._1, xx._2.map( xy =>
                                                                              (xy._1, ((xy._2 - xx._2.head._2)*100)/xx._2.head._2))));
            val flatBottomNArea2CumulativeJobGrowth=bottomNArea2JobCummulativeGrowth.flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
            flatBottomNArea2CumulativeJobGrowth.coalesce(1).saveAsTextFile(output_path + "/employment/cumulative_growth_in_low_employment_area");
            
            
            // Find yearly growth for top N
            val topNArea2JobYearlyGrowth = topNAreaName2JobCount.map ( xx =>
                                                      (xx._1, xx._2.map( xy =>
                                                         (xy._1,
                                                          if ( xx._2.indexOf(xy) == 0 ) 0
                                                          else ((xy._2 - xx._2.apply(xx._2.indexOf(xy) - 1)._2) * 100) / xx._2.apply(xx._2.indexOf(xy) - 1)._2)
                                                         )))
            val flatTopNArea2YearlyJobGrowth=topNArea2JobYearlyGrowth.flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
            flatTopNArea2YearlyJobGrowth.coalesce(1).saveAsTextFile(output_path + "/employment/yearly_growth_in_high_employment_area");
            
            
            // Find cumulative growth for top N
            val topNArea2JobCummulativeGrowth = topNAreaName2JobCount.map ( xx =>
                                                                              (xx._1, xx._2.map( xy =>
                                                                              (xy._1, ((xy._2 - xx._2.head._2)*100)/xx._2.head._2))));
            val flatTopNArea2CumulativeJobGrowth=topNArea2JobCummulativeGrowth.flatMapValues(xx=>xx).map(xx=> (xx._2._1, (xx._1, xx._2._2)) ).groupByKey().sortByKey()
            flatTopNArea2CumulativeJobGrowth.coalesce(1).saveAsTextFile(output_path + "/employment/cumulative_growth_in_high_employment_area");
        }
   }    
}


