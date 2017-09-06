//VESION 0.7 -- Breaking into hourly partitions
//0.8 -- stop checkpoint
//0.9 -- checkpoint restored. rememberwindow reduced to 50 seconds.
//10 - - Delete temp directories as first step within RDDs. 
//11-- undo 10. instead create new temp dirs
//12 -- undo checkpoint. undo picking old files. delete temp folder everyime.
//13 -- filter out blank XML RDDs
//14---same as 13. resubmitting
//15-- Modify input side to remove processed files
//15.b --- MAKE IT PRODUCTION TEST READY
//16 -- code change-- 
//17-- change batch duration to 40 seconds
//18 -- feeding data to DBO AC model
//19
//20
//21
//22
//23
//24
//25--temptable to hive (trying to fix write) //26 //27 //28  //29  //30
//31 issu resolved
//32 final testing
//33 batch duration from 30 to 60 sec for testing dev

import java.time.OffsetDateTime
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD
//import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.rdd.UnionRDD
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import java.util.Calendar
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.OffsetDateTime
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

object WAPLSparkStream {
  val conf = new SparkConf().setAppName("WAPL-PHD-Data-Streaming")
     val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._  
      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
case class queryData(tagno: String, value: String, confidence: String, timestamp1: String, processed_timestamp: String, tagname: String)
    
def tagNameMapping() : org.apache.spark.sql.DataFrame = {
var tagNamesDesired = Seq("W.30CY1575","W.30TC1563","W.30CASRAT1A","W.30FC150063","W.30FI1H15","W.30RY1H02","W.46fsof07a","W.30TC1H21",
"W.26TI130163","W.24LQTM07a", "W.30CY1592","W.30CASRAT1B","W.48EVAPRTTOT","W.41FC350080","W.30TC1H23","W.30CY1799","W.30P030282.POWER","W.30FFC1764","W.26DI1448","W.30FC1762",
"W.30CY1817", "W.30CASRAT2B","W.30FFC1H72","W.46thof07e","W.40EVD207B","W.43LPPTOTEVAP","W.40EVAP40.2","W.41FC350083")
//var tagNameString = tagNamesDesired.mkString("('","','", "')")
var tagNameInfo = hiveContext.sql("select TagID,Tagname from default.tagnamemapping")
val columnsRenamed = Seq("tagno", "tagname")
tagNameInfo = tagNameInfo.toDF(columnsRenamed: _*)
//val df3= tagNameInfo.filter($"tagname".isin(tagNamesDesired:_*))
return tagNameInfo
}
def averageTagName(splitFrame : org.apache.spark.sql.DataFrame ) : Array[(String, Double)] = {
val pairedRDD: RDD[(String, Double)] = splitFrame
.select($"tagname", $"value".cast("double"))
.as[(String, Double)].rdd
var avg_by_key = pairedRDD.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2).collect
println(avg_by_key)
// val tagValueRDD: Map[String, Long] = nameValue.map { case Row(tagname, value) => Map("tagname" -> tagname, "value" -> value) }

/* def multiFilter(words:List[String], line:String) = for { word <- words; if line.contains(word) } yield { (word,line) }
val filterWords = List("1","5","10")
val filteredRDD = rows.flatMap( line => multiFilter(filterWords, line) )
val groupedRDD = filteredRDD.groupBy(_._1) */
return avg_by_key
}

def joinFrame(df1 : org.apache.spark.sql.DataFrame, df2 : org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame={
val joinedFrame = df1.join(df2, "tagname")
return joinedFrame
}
def joinedTagNameID(dfRenamed : org.apache.spark.sql.DataFrame ) : org.apache.spark.sql.DataFrame = {
val hivetable = tagNameMapping()
println("hivetable------"+hivetable.count())
val joinedFrame = dfRenamed.join(hivetable, "tagno")// dfRenamed.as("d1").join(hivetable.as("d2"), $"d1.tagno" === $"d2.","left_outer")//dfRenamed.join(hivetable, "tagno")
return joinedFrame
}

def joinedTagNameWindow(dfRenamed : org.apache.spark.sql.DataFrame ) : org.apache.spark.sql.DataFrame = {
val hivetable = hiveContext.sql("select tagname,windowsec from default.tagdescription_temp")
val joinedFrame = dfRenamed.join(hivetable, "tagname")
return joinedFrame
}

def namedTextFileStream(ssc: StreamingContext, directory: String): DStream[String] =
ssc.fileStream[LongWritable, Text, TextInputFormat](directory).transform( rdd =>
new UnionRDD (rdd.context,rdd.dependencies.map( dep => dep.rdd.asInstanceOf[RDD[(LongWritable, Text)]].map(_._2.toString).setName(dep.rdd.name)
)
)
)
def transformByFile[U: ClassTag](unionrdd: RDD[String],transformFunc: String => RDD[String] => RDD[U]): RDD[U] = {
new UnionRDD(unionrdd.context,unionrdd.dependencies.map{ dep =>
if (dep.rdd.isEmpty) None
else {
val filename = dep.rdd.name
Some(transformFunc(filename)(dep.rdd.asInstanceOf[RDD[String]]).setName(filename)
)
}}.flatten
)
}

def createContext(batchduration_sec_str: String, rememberWindow_sec_str: String, inputDir: String, tmpDir: String,  outputPath: String, checkpointDirectory: String)
    : StreamingContext = {

 // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")
    
   
    //sc.setLogLevel("WARN")---------------------------------------------------------------------------------------
sc.hadoopConfiguration.setBoolean("parquet.enable.summary-metadata", false)
sc.hadoopConfiguration.setBoolean("spark.sql.hive.convertMetastoreParquet.mergeSchema",false)	
val batchduration_sec = batchduration_sec_str.toLong
val rememberWindow_sec = rememberWindow_sec_str.toLong
val ssc = new StreamingContext(sc, Seconds(batchduration_sec))
//ssc.checkpoint(checkpointDirectory)

val inputDirectory = inputDir
val tempDir = tmpDir
val dstream = namedTextFileStream(ssc, inputDirectory)

def byFileTransformer(filename: String)(rdd: RDD[String]): RDD[(String, String)] = rdd.map(line => (filename, line))
val transformed = dstream.transform(rdd => transformByFile(rdd, byFileTransformer))

transformed.foreachRDD {
  rdd =>
  rdd.cache()
  val hadoopConf = sc.hadoopConfiguration
  var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  if (!rdd.isEmpty()){
    val rdd1 = rdd.map(x => x._1)
    val rdd2 = rdd.map(x=> x._2)
    var path = tempDir
	val exists = hdfs.exists(new org.apache.hadoop.fs.Path(path))
            if(exists) {
                hdfs.delete(new org.apache.hadoop.fs.Path(path),true)
                }
    rdd2.saveAsTextFile(tempDir)
    val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "tag").load(tmpDir)
    if (df.rdd.isEmpty==false)
      {
      val flattened = df.withColumn("tagdata", explode($"tagdata"))
      val flattened1= flattened.withColumn( "processed_timestamp",lit(java.sql.Timestamp.from(java.time.Instant.now)))
      val df1 = flattened1.select($"_tagno", $"tagdata._timestamp", $"tagdata.value", $"tagdata.confidence",$"processed_timestamp",org.apache.spark.sql.functions.year($"processed_timestamp"),org.apache.spark.sql.functions.month($"processed_timestamp"),org.apache.spark.sql.functions.dayofmonth($"processed_timestamp"),org.apache.spark.sql.functions.hour($"processed_timestamp"))
     val newNames = Seq("tagno", "timestamp1", "value", "confidence","processed_timestamp","year","month","day","hour")
      val dfRenamed = df1.toDF(newNames: _*)
      val joinedFrame = joinedTagNameID(dfRenamed)
      //println("dfRenamed------"+dfRenamed.count())
      val df9= joinedFrame.select(joinedFrame("tagno").cast(StringType).as("tagno"),joinedFrame("tagname").cast(StringType).as("tagname"),joinedFrame("timestamp1").cast(StringType).as("timestamp1"),joinedFrame("value").cast(StringType).as("value"),joinedFrame("confidence").cast(StringType).as("confidence"),joinedFrame("processed_timestamp").cast(StringType).as("processed_timestamp"),joinedFrame("year").cast(StringType).as("year"),joinedFrame("month").cast(StringType).as("month"),joinedFrame("day").cast(StringType).as("day"),joinedFrame("hour").cast(StringType).as("hour"))
      df9.write.mode("append").partitionBy("year", "month", "day","hour").parquet(outputPath)
      //println("df9-----"+df9.count())
      //df9.show()
     // println("######################################################")
	 
      val dt=OffsetDateTime.now()
      val year =dt.getYear()
      val month = dt.getMonthValue()
      val day = dt.getDayOfMonth()
      val hour = dt.getHour()

      val dtx1= dt.minusHours(1)
      val y1=dtx1.getYear()
      val m1 = dtx1.getMonthValue()
      val d1 = dtx1.getDayOfMonth()
      val h1 = dtx1.getHour()

      val dtx2= dt.minusHours(2)
      val y2=dtx2.getYear()
      val m2 = dtx2.getMonthValue()
      val d2 = dtx2.getDayOfMonth()
      val h2 = dtx2.getHour()

      val dtx3= dt.minusHours(3)
      val y3=dtx3.getYear()
      val m3 = dtx3.getMonthValue()
      val d3 = dtx3.getDayOfMonth()
      val h3 = dtx3.getHour()

      val dtx4= dt.minusHours(4)
      val y4=dtx4.getYear()
      val m4 = dtx4.getMonthValue()
      val d4 = dtx4.getDayOfMonth()
      val h4 = dtx4.getHour()

    

      val basepath = outputPath
      val copypath = "year=" +year + "/month=" + month + "/day=" + day + "/hour=" + hour
      val finalpath = basepath+ copypath+ "/"

      val copypath1 = "year=" +y1+ "/month=" + m1 + "/day=" + d1 + "/hour=" + h1
      val finalpath1= basepath+ copypath1+ "/"

      val copypath2 = "year=" +y2+ "/month=" + m2 + "/day=" + d2 + "/hour=" + h2
      val finalpath2= basepath+ copypath2+ "/"

      val copypath3 = "year=" +y3+ "/month=" + m3 + "/day=" + d3 + "/hour=" + h3
      val finalpath3= basepath+ copypath3+ "/"

      val copypath4 = "year=" +y4+ "/month=" + m4 + "/day=" + d4 + "/hour=" + h4
      val finalpath4= basepath+ copypath4+ "/"

  
	
	
      var tagNamesDesired = Seq("W.30CY1575","W.30TC1563","W.30CASRAT1A","W.30FC150063","W.30FI1H15","W.30RY1H02","W.46fsof07a","W.30TC1H21",
      "W.26TI130163","W.24LQTM07a", "W.30CY1592","W.30CASRAT1B","W.48EVAPRTTOT","W.41FC350080","W.30TC1H23","W.30CY1799","W.30P030282.POWER","W.30FFC1764","W.26DI1448","W.30FC1762",
      "W.30CY1817", "W.30CASRAT2B","W.30FFC1H72","W.46thof07e","W.40EVD207B","W.43LPPTOTEVAP","W.40EVAP40.2","W.41FC350083")
	  
	  var path = finalpath
	  var path1 = finalpath1
	  var path2 = finalpath2
	  var path3 = finalpath3
	  var path4 = finalpath4
	 
      val exists = hdfs.exists(new org.apache.hadoop.fs.Path(path))
	  val exists1 = hdfs.exists(new org.apache.hadoop.fs.Path(path1))
	  val exists2 = hdfs.exists(new org.apache.hadoop.fs.Path(path2))
	  val exists3 = hdfs.exists(new org.apache.hadoop.fs.Path(path3))
	  val exists4 = hdfs.exists(new org.apache.hadoop.fs.Path(path4))
	 
	  val spark: SparkSession = SparkSession.builder.getOrCreate()
	  var dataframe = spark.emptyDataset[queryData].toDF()
            if(exists4 & exists3 & exists2 & exists1) {
                dataframe = sqlContext.read.option("basePath", basepath).parquet(finalpath,finalpath1,finalpath2,finalpath3,finalpath4).select($"tagno",$"timestamp1",$"value",$"confidence",$"processed_timestamp",$"tagname" )
                }
				else if(exists3 & exists2 & exists1)
				{
				dataframe = sqlContext.read.option("basePath", basepath).parquet(finalpath,finalpath1,finalpath2,finalpath3).select($"tagno",$"timestamp1",$"value",$"confidence",$"processed_timestamp",$"tagname" )
				}
				else if(exists2 & exists1)
				{
				dataframe = sqlContext.read.option("basePath", basepath).parquet(finalpath,finalpath1,finalpath2).select($"tagno",$"timestamp1",$"value",$"confidence",$"processed_timestamp",$"tagname" )
				}
				else if(exists1)
				{
				dataframe = sqlContext.read.option("basePath", basepath).parquet(finalpath,finalpath1).select($"tagno",$"timestamp1",$"value",$"confidence",$"processed_timestamp",$"tagname" )
				}
				else
				{
				dataframe = sqlContext.read.option("basePath", basepath).parquet(finalpath).select($"tagno",$"timestamp1",$"value",$"confidence",$"processed_timestamp",$"tagname" )
				}
     
      val filteredDF = dataframe.filter($"tagname".isin(tagNamesDesired:_*))
      //filteredDF.write.format("com.databricks.spark.csv").option("header", "true").save("wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/South32-poc/filteredDF.csv")
  
      val dedupDF = filteredDF.dropDuplicates(Seq("tagname","timestamp1","value","confidence"))
     // dedupDF.write.format("com.databricks.spark.csv").option("header", "true").save("wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/South32-poc/dedupe.csv")
  
      //println("#############data from blob end####################################")

      
       val timeInMillis = System.currentTimeMillis()
		val time = (timeInMillis/1000)
	   /* val desiredTime = "4/13/2017 5:55:00"
      val format = new java.text.SimpleDateFormat("M/dd/yyyy HH:m:ss")
      val time = format.parse(desiredTime).getTime()/1000*/
      val schema = StructType(
      StructField("tagname", StringType, false) ::
      StructField("avg", DoubleType, false) :: Nil)//cureent time

      //now join dedup with tag meta to get window seconds
      val frameToProcess = joinedTagNameWindow(dedupDF)
      
      //tags, which belongs to 5 mins frame
      var windowFive =frameToProcess.filter($"windowsec" === 5)
     // println("windowFive-------------------"+windowFive.show())
      windowFive = windowFive.withColumn("timestamp1", unix_timestamp($"timestamp1", "MM/dd/yyyy HH:mm:ss")).filter(( lit(time) - $"timestamp1")/60 <= 5)
      .select($"tagname", $"timestamp1".cast(TimestampType), $"value")
      //println("windowFive-------------------"+windowFive.show())

      //average for tags
      val avgWindowFive =  averageTagName(windowFive)
      //println("avgWindowFive---"+avgWindowFive)
      //println(avgWindowFive.take(5))
      val rddFive = sc.parallelize (avgWindowFive).map (x => org.apache.spark.sql.Row(x._1, x._2.asInstanceOf[Number].doubleValue()))
      //conversion of rdd to frame
      var dfFive = sqlContext.createDataFrame(rddFive, schema)
      //var joinFrameWithFive = joinFrame(frameToProcess,sqlContext.createDataFrame(rddFive, schema))
      //println("join five:--------------")
      //println(dfFive.show(2))

      //tags, which belongs to 1 mins frame
      var windowOne =frameToProcess.filter($"windowsec" === 1)
      //println("windowOne-------------------"+windowOne.show())
      windowOne = windowOne.withColumn("timestamp1", unix_timestamp($"timestamp1", "MM/dd/yyyy HH:mm:ss")).filter(( lit(time) - $"timestamp1")/60 <= 1)
      .select($"tagname", $"timestamp1".cast(TimestampType), $"value")
      //println("windowOne-------------------"+windowOne.show())
      var avgWindowOne = averageTagName(windowOne)
      //println("avgWindowOne---"+avgWindowOne)
      //println(avgWindowOne.take(5))
      val rddOne = sc.parallelize (avgWindowOne).map (x => org.apache.spark.sql.Row(x._1, x._2.asInstanceOf[Number].doubleValue()))
      var dfOneFive = dfFive.union(sqlContext.createDataFrame(rddOne, schema))
      //var joinFrameWithOne = joinFrame(joinFrameWithFive,sqlContext.createDataFrame(rddOne, schema))
      //println("join one:--------------")
      //println(dfOneFive.show(20))

      //tags,which belongs to 4 hrs frame
      var windowFour =frameToProcess.filter($"windowsec" === 240)
      //println("windowFour-------------------"+windowFour.show())
      //   windowFour = windowFour.withColumn("timestamp1", unix_timestamp($"timestamp1", "MM/dd/yyyy HH:mm:ss")).filter(( lit(time) - $"timestamp1")/60 <= 240)
      // .select($"tagname", $"timestamp1".cast(TimestampType), $"value")
      //println("windowFour-------------------"+windowFour.show())
      var avgWindowFour = averageTagName(windowFour)
      //println("avgWindowFour---"+avgWindowFour)
      //println(avgWindowFour.take(5))
      val rddFour = sc.parallelize (avgWindowFour).map (x => org.apache.spark.sql.Row(x._1, x._2.asInstanceOf[Number].doubleValue()))
      var dfOneFiveFour = dfOneFive.union(sqlContext.createDataFrame(rddFour, schema))
      //var joinFrameWithOne = joinFrame(joinFrameWithFive,sqlContext.createDataFrame(rddOne, schema))
      //println("join four hour:--------------")
      //println(dfOneFiveFour.show(20))

      //tags, which belongs to 10 mins frame
      var windowTen =frameToProcess.filter($"windowsec" === 10)
      windowTen = windowTen.withColumn("timestamp1", unix_timestamp($"timestamp1", "MM/dd/yyyy HH:mm:ss")).filter(( lit(time) - $"timestamp1")/60 <= 10 )
      .select($"tagname", $"timestamp1".cast(TimestampType), $"value")
      //println("windowTen-------------------"+windowTen.show())
      var avgWindowTen = averageTagName(windowTen)
      //println("avgWindowTen---"+avgWindowTen)
      //println(avgWindowTen.take(5))
      val rddTen = sc.parallelize (avgWindowTen).map (x => org.apache.spark.sql.Row(x._1, x._2.asInstanceOf[Number].doubleValue()))
      var dfOverall = dfOneFiveFour.union(sqlContext.createDataFrame(rddTen, schema))
      dfOverall.show()
      val tagname_cleaner = (s:String) => {if (s == null) null else s.replaceAll("\\.","\\_")}
      val tagname_cleaner_udf = udf(tagname_cleaner)
      dfOverall = dfOverall.withColumn("tagname", tagname_cleaner_udf(dfOverall("tagname")) )
      dfOverall.show()
    
      //println("data preperation for Hive..............")

      val dataToHive_interim = dfOverall.withColumn( "datetimeval",lit(java.sql.Timestamp.from(java.time.Instant.now)))
      dataToHive_interim.show()
     
      var newDF = dataToHive_interim.groupBy("datetimeval").pivot("tagname").agg(sum($"avg"))
      newDF.show()
      // newDF.write.format("com.databricks.spark.csv").option("header", "true").save("wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/South32-poc/debug-1.csv")
  
      // newDF=newDF.drop(newDF.col("tagname"))
      //newDF=newDF.drop(newDF.col("avg"))
      //println("after del tagname and avg: ")
      //newDF.printSchema
      val columns = Seq("W_30CY1575","W_30TC1563","W_30CASRAT1A","W_30FC150063","W_30FI1H15","W_30RY1H02","W_46fsof07a","W_30TC1H21","W_26TI130163","W_24LQTM07a","W_30CY1592","W_30CASRAT1B","W_48EVAPRTTOT","W_41FC350080","W_30TC1H23","W_30CY1799","W_30P030282_POWER","W_30FFC1764","W_26DI1448","W_30FC1762","W_30CY1817","W_30CASRAT2B","W_30FFC1H72","W_46thof07e","W_40EVD207B","W_43LPPTOTEVAP","W_40EVAP40_2","W_41FC350083")
      var columnsAdded = columns.foldLeft(newDF) { case (d, c) =>
      if (d.columns.contains(c)) {
          // column exists; skip it
          d
        } else {
          // column is not available so add it
          d.withColumn(c, lit(""))
        }
      }
      //columnsAdded.printSchema
      //columnsAdded.show()
      //println("after transpose, hope i get result... ")
      //newDF.show()
      newDF = columnsAdded.withColumn("ts_year", org.apache.spark.sql.functions.year($"datetimeval"))
      newDF.show()
      newDF = newDF.withColumn("ts_month",org.apache.spark.sql.functions.month($"datetimeval"))
      newDF.show()
      newDF = newDF.withColumn("ts_day",org.apache.spark.sql.functions.dayofmonth($"datetimeval"))
      newDF.show()
      newDF = newDF.withColumn("ts_hour",org.apache.spark.sql.functions.hour($"datetimeval"))
      newDF.show()
      newDF = newDF.withColumn("ts_minute",org.apache.spark.sql.functions.minute($"datetimeval"))
      newDF.show()
      // newDF.write.format("com.databricks.spark.csv").option("header", "true").save("wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/South32-poc/withoutAllTagsInPeriod.csv")
  
      newDF = newDF.withColumn("alltagsinperiod",lit(dfOverall.count))
      //newDF = newDF.withColumn("alltagsinperiod",lit(2))
      newDF.show()
      //newDF.printSchema
 // newDF.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/South32-poc/debugFinal.csv")
  newDF.write.insertInto("dboc_inputdataset")   
  // newDF.createOrReplaceTempView("mytempTable")        
     // hiveContext.sql("INSERT INTO TABLE dboc_inputdataset select * from mytempTable")//("create table mytable as select * from mytempTable") // ("INSERT INTO TABLE dboc_inputdataset as select * from mytempTable");
      //newDF.write.mode("append").format("parquet").saveAsTable("dboc_inputdataset")
      }
	  rdd1.distinct.collect.foreach {inputFile => 
          hdfs.delete(new org.apache.hadoop.fs.Path(inputFile),true)
          }
  }
}

ssc
}

def main (args: Array[String]): Unit = {
     
     /** if (args.length != 6) {
            System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
            System.err.println(
            """
              |Usage:  <batchduration_sec_str> <rememberWindow_sec_str> <inputDir> <tmpDir> <outputPath> <CheckpointDirectory>.
              |<batchduration_sec_str> describes how frequently the streaming will take up a new batch
              |remember window is how many seconds back will the streaming go to pick up a old file
              |input directory is where files are streamed from
              |filestream output is saved in a tmp dir
              |tmp dir is then read to actually parse the xml files and create RDD
              |tmp directory is overwritten
              |
              |The directory paths should be absolute path. Example: s"wasbs://waplhdinsightspark-defaultblob@s32auazeworprdhdiblob01.blob.core.windows.net/PHDDataIN/"
              |
            """.stripMargin
          )
          System.exit(1)
        }
      */
      //val Array(batchduration_sec_str, rememberWindow_sec_str, inputDir, tmpDir, outputPath, checkpointDirectory) = args
    
      val batchduration_sec_str= "60"
      val rememberWindow_sec_str = "40" //not used at the moment
      val inputDir= s"wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/PHDDataIN/"
      val tmpDir = s"wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/PHDDataINtmp2/"
      val outputPath = s"wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/SaveParquet_Partition3/"
     val checkpointDirectory = s"wasbs://waplhdinsightspark-ac1-defaultblob@s32auazewordevhdiblob02.blob.core.windows.net/ChekPoint3/"
      
      val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(batchduration_sec_str, rememberWindow_sec_str, inputDir, tmpDir, outputPath, checkpointDirectory))
      ssc.start()
      ssc.awaitTermination()
    

  }
}

     
