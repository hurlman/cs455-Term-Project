package cs455.spark.commmon

object ConstDefs {

   val NUM_ELEMENT_IN_TOP_OR_BOTTOM = 5

   val START_YEAR = 2010

   val END_YEAR = 2017

   val EMPLOYMENT_INPUT_DATA_DIR_NAME  = "/employment_data"

   val HOUSING_INPUT_DATA_DIR_NAME     = "/housing_data"

   val POPULATION_INPUT_DATA_DIR_NAME  = "/population_data"

   val MEDIAN_HOURLY_PAY_OUTPUT_DIR_NAME  = "/employment/median_avg_hourly_pay"

   val TOTAL_EMPLOYMENT_STAT_OUTPUT_DIR_NAME    = "/employment/all"

   val POPULATION_STAT_OUTPUT_DIR_NAME    = "/population/all"

   val HOUSING_STAT_OUTPUT_DIR_NAME    = "/housing/all"

   val REGRESSION_OUTPUT_DIR_NAME    = "/regression"

   val ALL_METRICS_OUTPUT_DIR_NAME    = "/all_metrics/"

   val ALL_METRICS_OUTPUT_FILE_NAME    = "all_metrics.csv"


   //exclude puertorico list.
   val Exclude_PR_List = List(38660,32420,25020,11640,10380,41980,41900)
}
