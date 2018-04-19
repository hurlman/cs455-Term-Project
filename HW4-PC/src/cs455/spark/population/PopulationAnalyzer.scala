package population

import org.apache.spark.sql._

class PopulationAnalyzer {
  def Execute(spark: SparkSession, input_path: String, output_path: String): Unit = {
    import spark.sqlContext.implicits._

    val popFile = spark.read.format("csv")
      .option("header", "true")
      .load(input_path + "/PEP_2017_GCTPEPANNR.US23PR_with_ann_MODIFIED.csv")

    popFile.createOrReplaceTempView("population")

    val popQuery = spark.sql(
      """
        |SELECT
        |   `GCtarget-geo-id2`, `GCdisplay-label2`, respop72010, respop72011, respop72012,
        |   respop72013, respop72014,respop72015, respop72016, respop72017
        |  FROM population
        |  WHERE `GCdisplay-label2` LIKE '% Metro Area%'
        |     AND `GCdisplay-label` LIKE '%United States - %'
      """.stripMargin)

    val popRDD = popQuery.rdd.map {
      case Row(regionID: String, regionName: String, p2010: String, p2011: String, p2012: String,
      p2013: String, p2014: String, p2015: String, p2016: String, p2017: String) =>
        s"$regionName($regionID)".replace(" Metro Area", "") ->
          IndexedSeq.apply(p2010.toInt, p2011.toInt, p2012.toInt, p2013.toInt, p2014.toInt,
            p2015.toInt, p2016.toInt, p2017.toInt)
    }
  }
}
