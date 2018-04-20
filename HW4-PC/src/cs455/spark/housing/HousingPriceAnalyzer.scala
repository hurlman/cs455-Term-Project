package cs455.spark.housing

import cs455.spark.util.Util._
import org.apache.spark.sql._

class HousingPriceAnalyzer extends java.io.Serializable {

  def Execute(spark: SparkSession, input_path: String, output_path: String, inputFile: Boolean): Unit = {
    import spark.sqlContext.implicits._

    val houseFile = spark.read.format("csv")
      .option("header", "true")
      .load(input_path + "/Metro_time_series.csv")

    val regionFile = spark.read.format("csv")
      .option("header", "true")
      .load(input_path + "/CountyCrossWalk_Zillow.csv")

    houseFile.createOrReplaceTempView("houses")
    regionFile.createOrReplaceTempView("regions")

    val housingPriceQuery = spark.sql(
      """
        |SELECT
        |   Date, RegionName, ZHVI_SingleFamilyResidence
        | FROM houses
        | WHERE Date LIKE '%-06-30%'
        |   AND (Date LIKE '%2010%'
        |   OR Date LIKE '%2011%'
        |   OR Date LIKE '%2012%'
        |   OR Date LIKE '%2013%'
        |   OR Date LIKE '%2014%'
        |   OR Date LIKE '%2015%'
        |   OR Date LIKE '%2016%'
        |   OR Date LIKE '%2017%')
      """.stripMargin)
    housingPriceQuery.createOrReplaceTempView("housingPrice")

    val regionsQuery = spark.sql(
      """
        |SELECT DISTINCT
        |   CBSACode, CBSAName
        |  FROM regions
      """.stripMargin)
    regionsQuery.createOrReplaceTempView("regionIDs")

    val housingPrices = spark.sql(
      """
        |SELECT
        |   regionIDs.CBSAName, housingPrice.RegionName, housingPrice.ZHVI_SingleFamilyResidence
        |  FROM housingPrice
        |  LEFT JOIN regionIDs
        |  ON housingPrice.RegionName=regionIDs.CBSACode
      """.stripMargin)

    val filteredHousingPrice = housingPrices.filter(
      $"CBSAName".isNotNull &&
        $"RegionName".isNotNull &&
        $"ZHVI_SingleFamilyResidence".isNotNull)

    val sortedRegionPrice = filteredHousingPrice.rdd.map {
      case Row(regionName: String, regionID: String, price: String) =>
        s"$regionName($regionID)" -> price.toInt
    }

    val yearlyRegionPrices = sortedRegionPrice
      .groupByKey()
      .filter(_._2.size == 8)
      .map(x => x._1 -> x._2.toIndexedSeq)

    val yearlyRegionGrowth = yearlyRegionPrices
      .map(x => x._1 -> CalculateGrowth(x._2))

    if (inputFile) {
      val sel = spark.sparkContext.textFile(input_path.
        replace("/housing_data","/selective_area.txt")).collect()

      val p = """(?<=\()[^)]+(?=\))""".r
      val selGrowth = yearlyRegionGrowth.filter(x => sel.contains(p.findFirstIn(x._1).getOrElse("-1")))
      val selTotals = yearlyRegionPrices.filter(x => sel.contains(p.findFirstIn(x._1).getOrElse("-1")))

      selGrowth
        .coalesce(1)
        .saveAsTextFile(output_path + "/housing/yearlyGrowth")
      selTotals
        .coalesce(1)
        .saveAsTextFile(output_path + "/housing/yearlyTotals")
    } else {

      val cumulativePriceDiffByRegion = yearlyRegionPrices
        .map(x =>
          x._1 -> (((x._2(7) - x._2(0)) / x._2(0).toDouble) * 100))

      val bottomFive = cumulativePriceDiffByRegion.takeOrdered(5)(Ordering[Double].on(_._2))
      val topFive = cumulativePriceDiffByRegion.takeOrdered(5)(Ordering[Double].reverse.on(_._2))

      val bot = bottomFive.map(_._1)
      val top = topFive.map(_._1)

      val botYearlyTotals = yearlyRegionPrices.filter(x => bot.contains(x._1))
      val topYearlyTotals = yearlyRegionPrices.filter(x => top.contains(x._1))

      val botYearlyGrowth = yearlyRegionGrowth.filter(x => bot.contains(x._1))
      val topYearlyGrowth = yearlyRegionGrowth.filter(x => top.contains(x._1))

      spark.sparkContext.parallelize(bottomFive)
        .coalesce(1)
        .saveAsTextFile(output_path + "/housing/botFiveCumulative")
      spark.sparkContext.parallelize(topFive)
        .coalesce(1)
        .saveAsTextFile(output_path + "/housing/topFiveCumulative")
      botYearlyTotals
        .coalesce(1)
        .saveAsTextFile(output_path + "/housing/botFiveYearlyTotals")
      topYearlyTotals
        .coalesce(1)
        .saveAsTextFile(output_path + "/housing/topFiveYearlyTotals")
      botYearlyGrowth
        .coalesce(1)
        .saveAsTextFile(output_path + "/housing/botFiveYearlyGrowth")
      topYearlyGrowth
        .coalesce(1)
        .saveAsTextFile(output_path + "/housing/topFiveYearlyGrowth")
    }

  }
}
