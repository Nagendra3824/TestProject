package com.orienit.nagendra.xx

import org.apache.spark.sql.SparkSession

object WordCount {
  
  def main(args: Array[String]): Unit = {
    
    
    val spark = SparkSession.builder().master("local").appName("prasad spark").getOrCreate()
    
    
    val text = spark.sparkContext.textFile("/home/kalyan/work/hi")
    val counts = text.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
    counts.saveAsTextFile("/home/kalyan/work/op1")
    
  }
  
  
  
}