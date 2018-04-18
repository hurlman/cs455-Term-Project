package housing

import org.apache.spark.sql._

class HousingPriceAnalyzer extends java.io.Serializable {

  def Execute(spark: SparkSession, input_path: String, output_path: String): Unit = {
    import spark.sqlContext.implicits._

    val houseFile = spark.read.format("csv")
      .option("header", "true")
      .load(input_path + "/Metro_time_series.csv")

    houseFile.createOrReplaceTempView("houses")

    val result: DataFrame = spark.sql(
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

    val filteredResult = result.filter(
      $"Date".isNotNull &&
        $"RegionName".isNotNull &&
        $"ZHVI_SingleFamilyResidence".isNotNull)

    val kvp = filteredResult.rdd.map {
      case Row(date: String, region: String, price: String) =>
        region -> price.toInt
    }

    val yearlyMetro = kvp
      .groupByKey()
      .filter(_._2.size == 8)
      .map(x => x._1 -> x._2.toIndexedSeq)

    val cumulativePriceDiff = yearlyMetro
      .map(x =>
        x._1 -> (((x._2(7) - x._2(0))/x._2(0).toDouble)*100))

    val bottomFive = cumulativePriceDiff.takeOrdered(5)(Ordering[Double].on(_._2))
    val topFive = cumulativePriceDiff.takeOrdered(5)(Ordering[Double].reverse.on(_._2))
  }
}
