package org.inceptez.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;
//case class customer(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)
    
object objsql2 {
case class transcls (transid:String,transdt:String,custid:String,salesamt:Float,category:String,prodname:String,state:String,city:String,payment:String)  
  def main(args:Array[String])
  {
/*
SQLContext is a class and is used for initializing the functionalities of Spark SQL. 
SparkContext class object (sc) is required for initializing SQLContext class object.
 */
    val conf = new SparkConf().setAppName("SQL1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    sc.setLogLevel("ERROR");

/*Spark 2.x, we have a new entry point for DataSet and Dataframe API’s called as Spark Session.
SparkSession is essentially combination of SQLContext, HiveContext and future StreamingContext. 
All the API’s available on those contexts are available on spark session also. 
Spark session internally has a spark context for actual computation.*/

    
val spark=SparkSession.builder().appName("Sample sql app").master("local[*]").getOrCreate();

println("Creating Dataset")   

import sqlc.implicits._  
    val ds = spark.read.json("file:/home/hduser/hive/data/txns").as[transcls]
    
     val ds1 = ds.where("state == 'Texas'")
    
    ds1.take(10).foreach(println)
    ds1.printSchema()
    ds1.show()
    
    //convert dataset to dataframe
    val df = ds1.toDF()
    
    //convert dataset to rdd
    val rdd = ds1.rdd
    
println("Creating dataframe using jdbc connector for DB")
    
val dfdb = spark.read.format("jdbc")
       .option("url", "jdbc:mysql://localhost/custdb")
       .option("driver", "com.mysql.jdbc.Driver")
       .option("dbtable", "customer")
       .option("user", "root")
       .option("password", "root")
       .load() 
  
val dfdb1 = dfdb.where("city === 'chennai'")
    dfdb1.show()   
    

    
  }
 
  
}