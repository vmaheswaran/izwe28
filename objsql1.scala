package org.inceptez.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;
case class customer(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)
object sql1 {
  
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

    // Various ways of creating dataframes

// 1.Create dataframe using case class  with toDF and createdataframe functions (Reflection) 
// 2.create dataframe from collections type such as structtype and fields        
// 3.creating dataframe from read method using csv

    val arr:List[(String,Int)] = List(("Tiger",20),("Lion",10),("Elephant",40),("Leopard",12))
    val rdd = sc.parallelize(arr)
    import sqlc.implicits._
    
    val df = arr.toDF("animalname","count")
     
    println("Printing DF1 List to DF Example");
  
    df.select("count","animalname").show(10);
    df.printSchema()
    df.show()
    
    /* Create dataframe using case class  with toDF and createdataframe functions*/
    println("Create dataframe using case class from file")
    
    //step 1: import the implicits
    //import sql.implicits._
    
    //step 2: create rdd
    val filerdd = sc.textFile("file:/home/hduser/hive/data/custs")
    
    //step 3: create case class based on the structure of the data
val rdd1 = filerdd.map(x => x.split(",")).filter(x => x.length == 5)
.map(x => customer(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
               
    val filedf = rdd1.toDF()
    
    //implicits is not required for the createDataFrame function.
    val filedf1 = sqlc.createDataFrame(rdd1)
    
    filedf.printSchema()
    filedf.show(10,false)
    
    //creating dataframe from createdataframe
    
    println("creating dataframe using createdataframe")
    
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types._
    
val rddrow = filerdd.map(x => x.split(",")).filter(x => x.length == 5)
.map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    

// use collections, since a schema is built as a StructType(Array[StructField]). So it's basically a choice between tuples and collections. 

    val structypeschema = StructType(List(StructField("custid", IntegerType, true),
        StructField("custfname", StringType, true),
        StructField("custlname", StringType, true),
        StructField("custage", IntegerType, true),
        StructField("custprofession", StringType, true)))
    
   val dfcreatedataframe = sqlc.createDataFrame(rddrow, structypeschema)
   
   dfcreatedataframe.select("*").filter("custprofession='Pilot' and custage>35").show(10);
    
    
    //creating dataframe from read method using csv
    println("creating dataframe from read method using csv")
    
    val dfcsv = sqlc.read.format("csv")
    //.option("header",true)
    .option("delimiter",",")
    .option("inferSchema",true)
    .load("file:/home/hduser/hive/data/custs")

    dfcsv.show(10);
    dfcsv.printSchema();
    
    val onecoldfcsv=dfcsv.withColumnRenamed("_c0", "custid")
    val onecoldfcsv1=dfcsv.withColumnRenamed("_c0", "custid").drop("_c1","_c2")
    
    import org.apache.spark.sql.functions._
    val withcoldf = dfcsv.withColumn("pilot?",$"_c4".contains("Pilot"))
    .withColumn("Dataset",lit("Transactions"))//.withColumn("full name",concat(dfcsv("_c1"),dfcsv("_c2")))
    println("withcoldf")
    withcoldf.show()
    
    val multiplecoldfcsv=dfcsv.select($"_c0".alias("custid"),$"_c1".alias("Custname"))
    val onecoldfcsvexplain=dfcsv.withColumnRenamed("_c0", "custid").drop("_c1","_c2").explain(true)
    
    println("dfcsv")
    dfcsv.printSchema();
    println("onecoldfcsv")
    onecoldfcsv.printSchema();
    println("onecoldfcsv1")
    onecoldfcsv1.printSchema();
    println("multiplecoldfcsv")
    multiplecoldfcsv.printSchema();
    //creating dataframe from read method using csv with columns explicitly defined
    println("creating dataframe from read method using csv with columns explicitly defined")
    
    val dfcsvcolumns = sqlc.read.format("csv")
    //.option("header",true)
    .option("delimiter",",")
    .option("inferSchema",true)
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","custfname","custlname","custage","custprofession")
  
    val dfgrouping = dfcsvcolumns.groupBy("custprofession")
    .agg(max("custage").alias("maxage"),min("custage").alias("minage"))
    println("dfgrouping")
    dfgrouping.show
    
    dfcsvcolumns.createOrReplaceTempView("customer")
    
    val df1 = sqlc.sql("""select custprofession, count(custid) from customer 
                        where custage > 50 group by custprofession""")
    
    val df2 = sqlc.sql("""select custid,custfname,custlname,case when (custage < 40) then 'Middle Aged' else 'Old Age' end as agegrp 
                        from customer""")
    
    println("Spark SQL customer info")                        
    df2.show()
    
    
    
    val txns=spark.read.option("inferschema",true)
    .option("header",false).option("delimiter",",")
    .csv("file:///home/hduser/hive/data/txns")
    .toDF("txnid","dt","cid","amt","category","product","city","state","transtype")
    
txns.show(10);    


    
println("Join Scenario in temp views")
txns.createOrReplaceTempView("trans");
    
val dfjoinsql = sqlc.sql("""select a.custid,custprofession,b.amt,b.transtype  
  from customer a inner join trans b
  on a.custid=b.cid""")
  
  
  
  dfjoinsql.show(10)

  }
 
  
}