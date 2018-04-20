package cs455.spark.util

object Util {

  def CalculateGrowth(yearlyData: IndexedSeq[Int]): IndexedSeq[Double] ={
    val growth = new Array[Double](yearlyData.length)
    for(i <- growth.indices){
      if(i == 0) growth(i) = 0
      else growth(i) = ((yearlyData(i) - yearlyData(i-1)) * 100.0) / yearlyData(i-1)
    }
    growth
  }
}
