package cs455.spark.model
import org.apache.spark.SparkContext
import java.io._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql._
import cs455.spark.commmon.ConstDefs._

class LinearRegresser extends java.io.Serializable 
{
    val m_ColumNames = Seq("area_name", "year", "housing", "total_employment", "population", "medavghourlyearning")
    def Execute(spark: SparkSession, input_root:String, output_path:String):Unit = 
    {
       val input_path = input_root + ALL_METRICS_OUTPUT_DIR_NAME + ALL_METRICS_OUTPUT_FILE_NAME
       val Results = List (                                                                                                                            //iter, reg,area,  year,  max price
             ("all_area_2010-2017_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "00000", "0000", 0)),
             ("all_area_2010-2017_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "00000", "0000", 0)),
             ("all_area_2010-2017_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "00000", "0000", 0)),
             ("all_area_2010-2017_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "00000", "0000", 0)),
             ("all_area_2010-2017_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "00000", "0000", 0)),

             ("all_area_2016_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "00000", "2016", 0)),
             ("all_area_2016_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "00000", "2016", 0)),
             ("all_area_2016_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "00000", "2016", 0)),
             ("all_area_2016_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "00000", "2016", 0)),
             ("all_area_2016_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "00000", "2016", 0)),


             ("44700_area_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "44700", "0000", 0)),
             ("44700_area_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "44700", "0000", 0)),
             ("44700_area_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "44700", "0000", 0)),
             ("44700_area_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "44700", "0000", 0)),
             ("44700_area_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "44700", "0000", 0)),

             ("46700_area_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "46700", "0000", 0)),
             ("46700_area_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "46700", "0000", 0)),
             ("46700_area_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "46700", "0000", 0)),
             ("46700_area_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "46700", "0000", 0)),
             ("46700_area_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "46700", "0000", 0)),



             ("39900_area_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "39900", "0000", 0)),
             ("39900_area_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "39900", "0000", 0)),
             ("39900_area_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "39900", "0000", 0)),
             ("39900_area_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "39900", "0000", 0)),
             ("39900_area_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "39900", "0000", 0)),


             ("37980_area_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "37980", "0000", 0)),
             ("37980_area_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "37980", "0000", 0)),
             ("37980_area_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "37980", "0000", 0)),
             ("37980_area_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "37980", "0000", 0)),
             ("37980_area_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "37980", "0000", 0)),

             ("15980_area_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "15980", "0000", 0)),
             ("15980_area_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "15980", "0000", 0)),
             ("15980_area_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "15980", "0000", 0)),
             ("15980_area_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "15980", "0000", 0)),
             ("15980_area_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "15980", "0000", 0)),


             ("41180_area_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "41180", "0000", 0)),
             ("41180_area_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "41180", "0000", 0)),
             ("41180_area_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "41180", "0000", 0)),
             ("41180_area_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "41180", "0000", 0)),
             ("41180_area_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "41180", "0000", 0)),


             ("35620_area_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "35620", "0000", 0)),
             ("35620_area_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "35620", "0000", 0)),
             ("35620_area_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "35620", "0000", 0)),
             ("35620_area_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "35620", "0000", 0)),
             ("35620_area_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "35620", "0000", 0)),


             ("14500_area_population-total_employment_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "population", "total_employment"    , 500, .3, "14500", "0000", 0)),
             ("14500_area_population-medavghourlyearning_reg_.3_iter_500:" +  RunModelForStatCollection(spark, input_path, "population", "medavghourlyearning" , 500, .3, "14500", "0000", 0)),
             ("14500_area_housing-population_reg_.3_iter_500            :" +  RunModelForStatCollection(spark, input_path, "housing",    "population"          , 500, .3, "14500", "0000", 0)),
             ("14500_area_housing-total_employment_reg_.3_iter_500      :" +  RunModelForStatCollection(spark, input_path, "housing",    "total_employment"    , 500, .3, "14500", "0000", 0)),
             ("14500_area_housing-medavghourlyearning_reg_.3_iter_500   :" +  RunModelForStatCollection(spark, input_path, "housing",    "medavghourlyearning" , 500, .3, "14500", "0000", 0))

             )
        spark.sparkContext.parallelize(Results).coalesce(1).saveAsTextFile(output_path + REGRESSION_OUTPUT_DIR_NAME);
    } 



    def RunModelForStatCollection(spark: SparkSession, input_path:String, label_name:String, feature_name:String,
                                   num_iter : Int, reg_param : Double,
                                   area_code : String, year_filter: String,  max_median_price: Double):String = {
       import spark.implicits._;
       println (s"input_path:${input_path}, label_name: ${label_name}, feature_name:${feature_name}")
       val input = spark.sparkContext.textFile(input_path)
       val header = input.first()
       val inputData = input.filter(xx=> xx!= header);
       val data = inputData.map(_.split(","))
                           .map(x => (x(0), x(1), if( label_name == "housing") x(2).toDouble/10000 else x(2).toDouble, x(3).toDouble, 
                            if( label_name == "population") x(4).toDouble/10000 else x(4).toDouble, x(5).toDouble))
                           .filter( if( year_filter == "0000") (_._2 != year_filter )     else (_._2 == year_filter))
                           .filter( if( max_median_price == 0) (_._3 > max_median_price)  else (_._3 < max_median_price))
                           .filter( if( area_code == "00000")  (_._1 != area_code)        else (_._1 == area_code ))
       val inputDF = data.toDF(m_ColumNames:_*)
       val label2FeaturesData = inputDF.select(label_name, feature_name);
       val label2FeaturesRegressData = new VectorAssembler().setInputCols(Array(feature_name)).setOutputCol("features").transform(label2FeaturesData).cache()
       val regession = new LinearRegression().setFeaturesCol("features").setLabelCol(label_name).setMaxIter(num_iter).setPredictionCol("pred_prop").setRegParam(reg_param)
       val model = regession.fit(label2FeaturesRegressData);
       println (s"Coefficients: ${model.coefficients}, intercept: ${model.intercept}, R2: ${model.summary.r2}, RMSE: ${model.summary.rootMeanSquaredError}")
       s"Coefficients: ${model.coefficients}, intercept: ${model.intercept}, R2: ${model.summary.r2}, RMSE: ${model.summary.rootMeanSquaredError}"
      }
}
