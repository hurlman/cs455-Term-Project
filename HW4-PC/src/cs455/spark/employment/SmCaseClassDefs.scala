package cs455.spark.employment
  
//Sm area case class 
case class SmArea(area_code:Int, area:String)

//Sm Series case class 
case class SmSeries(series_id: String, state_code: Int, area_code: Int, 
                    supersector_code: Int, industry_code: Int, data_type_code: Int, 
                    seasonal: String, benchmark_year: Int, footnote_codes: String, 
                    begin_year: Int, begin_period: String, end_year: Int, end_period: String);

//Sm Data case class 
case class SmDataSeries(series_id: String, year: Int, period:String, value:Int, footnode_codes:String)

