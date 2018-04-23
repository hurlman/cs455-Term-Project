package cs455.spark.population

import org.apache.spark.sql._
import cs455.spark.util.Util._
import cs455.spark.commmon.ConstDefs._

class PopulationAnalyzer {
  def Execute(spark: SparkSession, input_path: String, output_path: String, inputFile: Boolean): Unit = {
    import spark.sqlContext.implicits._

    //Read file into Dataframe
    val popFile = spark.read.format("csv")
      .option("header", "true")
      .load(input_path + "/PEP_2017_GCTPEPANNR.US23PR_with_ann_MODIFIED.csv")

    popFile.createOrReplaceTempView("population")

    //Use SparkSQL to select appropriate data
    val popQuery = spark.sql(
      """
        |SELECT
        |   `GCtarget-geo-id2`, `GCdisplay-label2`, respop72010, respop72011, respop72012,
        |   respop72013, respop72014, respop72015, respop72016, respop72017
        |  FROM population
        |  WHERE `GCdisplay-label2` LIKE '% Metro Area%'
        |     AND `GCdisplay-label` LIKE '%United States - %'
      """.stripMargin)

    //Remove rows with null values
    val filteredPopQuery = popQuery.filter(
      $"GCtarget-geo-id2".isNotNull &&
        $"GCdisplay-label2".isNotNull &&
        $"respop72010".isNotNull &&
        $"respop72011".isNotNull &&
        $"respop72012".isNotNull &&
        $"respop72013".isNotNull &&
        $"respop72014".isNotNull &&
        $"respop72015".isNotNull &&
        $"respop72016".isNotNull &&
        $"respop72017".isNotNull
    )

    // Let's collect the stat.  
    filteredPopQuery.drop(filteredPopQuery.col("GCdisplay-label2")).rdd.map {
      case Row(regionID: String, p2010: String, p2011: String, p2012: String,
      p2013: String, p2014: String, p2015: String, p2016: String, p2017: String) =>
        s"$regionID" ->
          IndexedSeq.apply(p2010.toInt, p2011.toInt, p2012.toInt, p2013.toInt, p2014.toInt,
            p2015.toInt, p2016.toInt, p2017.toInt)
    }.coalesce(1).saveAsTextFile(output_path + POPULATION_STAT_OUTPUT_DIR_NAME)


    //Convert to RDD
    val yearlyPop = filteredPopQuery.rdd.map {
      case Row(regionID: String, regionName: String, p2010: String, p2011: String, p2012: String,
      p2013: String, p2014: String, p2015: String, p2016: String, p2017: String) =>
        s"$regionName($regionID)".replace(" Metro Area", "") ->
          IndexedSeq.apply(p2010.toInt, p2011.toInt, p2012.toInt, p2013.toInt, p2014.toInt,
            p2015.toInt, p2016.toInt, p2017.toInt)
    }

    //Calculate growth and cumulative differences for each metro
    val yearlyPopGrowth = yearlyPop.map(x => x._1 -> CalculateGrowth(x._2))
    val cumulativePopDiff = yearlyPop.map(x => x._1 -> (((x._2(7) - x._2(0)) / x._2(0).toDouble) * 100))

    //gather data on metros in input file
    if (inputFile) {
      val sel = spark.sparkContext.textFile(input_path.
        replace("/population_data", "/selective_area.txt")).collect()

      val p = """(?<=\()[^)]+(?=\))""".r
      val selGrowth = yearlyPopGrowth.filter(x => sel.contains(p.findFirstIn(x._1).getOrElse("-1")))
      val selTotals = yearlyPop.filter(x => sel.contains(p.findFirstIn(x._1).getOrElse("-1")))
      val selDiff = cumulativePopDiff.filter(x => sel.contains(p.findFirstIn(x._1).getOrElse("-1")))

      selGrowth
        .coalesce(1)
        .saveAsTextFile(output_path + "/population/yearlyGrowth")
      selTotals
        .coalesce(1)
        .saveAsTextFile(output_path + "/population/yearlyTotals")
      selDiff
        .coalesce(1)
        .sortBy(_._2)
        .saveAsTextFile(output_path + "/population/cumulativeDiff")
    }
    else {
      //Or all only top and bottom cumulative metros

      val bottomFive = cumulativePopDiff.takeOrdered(5)(Ordering[Double].on(_._2))
      val topFive = cumulativePopDiff.takeOrdered(5)(Ordering[Double].reverse.on(_._2))

      val bot = bottomFive.map(_._1)
      val top = topFive.map(_._1)

      val botYearlyTotals = yearlyPop.filter(x => bot.contains(x._1))
      val topYearlyTotals = yearlyPop.filter(x => top.contains(x._1))

      val botYearlyGrowth = yearlyPopGrowth.filter(x => bot.contains(x._1))
      val topYearlyGrowth = yearlyPopGrowth.filter(x => top.contains(x._1))

      spark.sparkContext.parallelize(bottomFive)
        .coalesce(1)
        .saveAsTextFile(output_path + "/population/botFiveCumulative")
      spark.sparkContext.parallelize(topFive)
        .coalesce(1)
        .saveAsTextFile(output_path + "/population/topFiveCumulative")
      botYearlyTotals
        .coalesce(1)
        .saveAsTextFile(output_path + "/population/botFiveYearlyTotals")
      topYearlyTotals
        .coalesce(1)
        .saveAsTextFile(output_path + "/population/topFiveYearlyTotals")
      botYearlyGrowth
        .coalesce(1)
        .saveAsTextFile(output_path + "/population/botFiveYearlyGrowth")
      topYearlyGrowth
        .coalesce(1)
        .saveAsTextFile(output_path + "/population/topFiveYearlyGrowth")
    }
  }
}
