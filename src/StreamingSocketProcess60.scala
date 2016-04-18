package cn.sitech

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.io.Source

object StreamingSocketProcess60 extends Logging {
  private val process: ProcessSums = new ProcessSums()

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def competeTime(Stime: Long, prefix: String = ""): Unit = {
    val end_time = System.currentTimeMillis
    val cost_time = end_time.-(Stime)
    logInfo(prefix + " cost time :" + cost_time.toString + "----------------")
  }

  private val hadoopConf = new Configuration()
  private val fs: FileSystem = FileSystem.get(hadoopConf)
  def readFromHDFS(spath: String): Array[Byte] ={
    val in = fs.open(new Path(spath))
    val buf = ListBuffer[Byte]()
    var b = in.read()
    while (b != -1) {
      buf.append(b.byteValue)
      b = in.read()
    }
    buf.toArray
  }

  def readFromHDFS11(spath: String):  HashMap[String,String]  ={
    val data= Source.fromBytes(readFromHDFS(spath),"utf-8").getLines()
    var line=""
    val codeMap =new HashMap[String,String]
    while  (data.hasNext){
      line=data.next()
      val tmp = line.split("\t")
      if (tmp.length == 3) {
        codeMap(tmp(0) + "," + tmp(1)) = tmp(2)
      }
    }
    logInfo(spath+": -------size-------"+codeMap.size)
    codeMap
  }

  def readFromHDFS22(spath: String):  HashMap[String,String]  ={
    val data= Source.fromBytes(readFromHDFS(spath),"utf-8").getLines()
    var line=""
    val codeMap =new HashMap[String,String]
    while  (data.hasNext){
      line=data.next()
      val tmp = line.split("\t")
      // if (tmp.length == 7) {
      codeMap(tmp(1) + "," + tmp(2)) = tmp(5)
      // }
    }
    logInfo(spath+": -------size-------"+codeMap.size)
    codeMap
  }


  def createContext(args: Array[String],checkpointDirectory:String) = {

    //  initialization
    val in = new FileInputStream(args(0))
    val PROPERTIES = new Properties()
    PROPERTIES.load(in)

    val masterUrl = PROPERTIES.getProperty("spark.master.url")
    //    val socketHost = PROPERTIES.getProperty("socket.hostname")
    //    val socketPort = PROPERTIES.getProperty("socket.port").toInt
    val socketHost=args(1)
    val socketPort=args(2)

    val batchInterval = Milliseconds(PROPERTIES.getProperty("spark.streaming.batchInterval").toInt)
    val ftpServer = PROPERTIES.getProperty("store.server")
    val ftpServerUsername = PROPERTIES.getProperty("store.server.username")
    val ftpServerPasswd = PROPERTIES.getProperty("store.server.passwd")
    val ftpServerPath = PROPERTIES.getProperty("store.server.path")
    val hdfsServerPath = PROPERTIES.getProperty("hdfs.server.path")
    val cache = PROPERTIES.getProperty("spark.streaming.write-cache").toInt
    val code_file_yaxin=PROPERTIES.getProperty("code.file.yaxin")
    val code_file_yiyang=PROPERTIES.getProperty("code.file.yiyang")
    val retryTime = 3

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("StreamingSocketProcess60-67-68-69-70")
    sparkConf.set("spark.streaming.concurrentJobs", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.set("spark.cleaner.ttl", "12000")
    //sparkConf.set("spark.default.parallelism","400")
    //sparkConf.set("spark.storage.memoryFraction","0.5")
    sparkConf.set("spark.streaming.unpersist","true")
    sparkConf.set("spark.shuffle.consolidateFiles","true")
    sparkConf.set("spark.akka.askTimeout", "120")
    val ssc = new StreamingContext(sparkConf, batchInterval)
    ssc.checkpoint(checkpointDirectory)

    //
    //    //  读取hdfs上文件 yaxin
    //    val yaxin_codeMap = {
    //      val codeMap1=readFromHDFS11(code_file_yaxin)
    //      ssc.sparkContext.broadcast(codeMap1)
    //    }
    //    //  读取hdfs上文件 yiyang
    //    val yiyang_codeMap = {
    //      val codeMap2=readFromHDFS22(code_file_yiyang)
    //      ssc.sparkContext.broadcast(codeMap2)
    //    }

    val line1 =  ssc.socketTextStream(socketHost, socketPort.toInt, StorageLevel.MEMORY_AND_DISK_SER_2)
    val lines = line1.window(Milliseconds(200000))

    lines.foreachRDD(rdd => {
      if(!rdd.partitions.isEmpty){
        logInfo("Received --------- " + rdd.count().toString + " --------- UnCleaned socket lines.")
      }else{
        logInfo("Received --------- 0 ---------- socket lines.")
      }
    })

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    //源数据存储HDFS
    val s60= lines.filter(_.startsWith("60"))
    //s60.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s60/" + sdf.format(new Date(System.currentTimeMillis()))+"/")
    s60.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        logInfo(" No 60_Data in this batchInterval --------")
      } else {
        val startTime = System.currentTimeMillis()
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val time = sdf.format(new Date(startTime))
        rdd.saveAsTextFile("hdfs://sparkCluster/xinling/socket/source/s60/" + time +"/"+startTime)
        competeTime(startTime, "Processed 60_Data write to HDFS")
      }
    })

    val s67= lines.filter(_.startsWith("67"))
    //s67.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s67/" + sdf.format(new Date(System.currentTimeMillis()))+"/")
    s67.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        logInfo(" No 67_Data in this batchInterval --------")
      } else {
        val startTime = System.currentTimeMillis()
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val time = sdf.format(new Date(startTime))
        rdd.saveAsTextFile("hdfs://sparkCluster/xinling/socket/source/s67/" + time +"/"+startTime)
        competeTime(startTime, "Processed 67_Data  write to HDFS")
      }
    })

    val s68= lines.filter(_.startsWith("68"))
    //s68.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s68/" + sdf.format(new Date(System.currentTimeMillis()))+"/")
    s68.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        logInfo(" No 68_Data in this batchInterval --------")
      } else {
        val startTime = System.currentTimeMillis()
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val time = sdf.format(new Date(startTime))
        rdd.saveAsTextFile("hdfs://sparkCluster/xinling/socket/source/s68/" + time +"/"+startTime)
        competeTime(startTime, "Processed 68_Data write to HDFS")
      }
    })

    val s69= lines.filter(_.startsWith("69"))
    //s69.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s69/" + sdf.format(new Date(System.currentTimeMillis()))+"/")
    s69.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        logInfo(" No 69_Data in this batchInterval --------")
      } else {
        val startTime = System.currentTimeMillis()
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val time = sdf.format(new Date(startTime))
        rdd.saveAsTextFile("hdfs://sparkCluster/xinling/socket/source/s69/" + time +"/"+startTime)
        competeTime(startTime, "Processed 69_Data write to HDFS")
      }
    })

    val s70= lines.filter(_.startsWith("70"))
    //s70.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s70/" + sdf.format(new Date(System.currentTimeMillis()))+"/")
    s70.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        logInfo(" No 70_Data in this batchInterval --------")
      } else {
        val startTime = System.currentTimeMillis()
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val time = sdf.format(new Date(startTime))
        rdd.saveAsTextFile("hdfs://sparkCluster/xinling/socket/source/s70/" + time +"/"+startTime)
        competeTime(startTime, "Processed 70_Data write to HDFS")
      }
    })

    //    // 调用HDFS接口存储   原始数据入库
    //    lines.foreachRDD(rdd => {
    //      if (rdd.partitions.isEmpty) {
    //        logInfo(" No Data in this batchInterval --------")
    //      } else {
    //        val start_time = System.currentTimeMillis()
    //        rdd.mapPartitionsWithIndex((index,iter) => {
    //          val res2 = List[(String)]()
    //          process.writeAsPartitionToHDFS(index,iter, hdfsServerPath,cache)
    //          res2.iterator
    //        }).collect() //rdd.mapPartitionsWithIndex=>Iterator
    //        competeTime(start_time, "Processed data write to HDFS")
    //      }
    //    })

    ssc
  }

  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println("Usage:<conf> <host> <port> <checkpoint> ")
      System.exit(1)
    }

    val checkpointDirectory ="/xinling/checkpoint_"+args(3)
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(args,checkpointDirectory)
      })
    ssc.start()
    ssc.awaitTermination()

  }
}
