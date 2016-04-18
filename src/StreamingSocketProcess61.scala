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

object StreamingSocketProcess61 extends Logging {
  private val process: ProcessSums = new ProcessSums()
  private val process61: Process = new Process(Array(12,15),97,1,23,24,"2g_call")
  private val process62: Process = new Process(Array(12,15),91,1,23,24,"3g_call")
  private val process65: Process = new Process(Array(13,16),61,1,21,22,"2g_sms")
  private val process66: Process = new Process(Array(13,16),61,1,21,22,"3g_sms")

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

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("StreamingSocketProcess61-62-65-66").set("spark.driver.allowMultipleContexts", "true")
    sparkConf.set("spark.streaming.concurrentJobs", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.set("spark.cleaner.ttl", "12000")
    //sparkConf.set("spark.default.parallelism","400")
    sparkConf.set("spark.storage.memoryFraction","0.5")
    sparkConf.set("spark.streaming.unpersist","true")
    val ssc = new StreamingContext(sparkConf, batchInterval)
    ssc.checkpoint(checkpointDirectory)

    //  读取hdfs上文件 yaxin
    val yaxin_codeMap = {
      val codeMap1=readFromHDFS11(code_file_yaxin)
      ssc.sparkContext.broadcast(codeMap1)
    }
    //  读取hdfs上文件 yiyang
    val yiyang_codeMap = {
      val codeMap2=readFromHDFS22(code_file_yiyang)
      ssc.sparkContext.broadcast(codeMap2)
    }

    val lines =  ssc.socketTextStream(socketHost, socketPort.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    lines.persist(StorageLevel.MEMORY_AND_DISK_SER)
    lines.foreachRDD(rdd => {
      if(!rdd.partitions.isEmpty){
        logInfo("Received --------- " + rdd.count().toString + " --------- UnCleaned socket lines.")
      }else{
        logInfo("Received --------- 0 ---------- socket lines.")
      }
    })

    val startTime1 = System.currentTimeMillis()
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val dt1 = sdf.format(new Date(startTime1))

    //源数据存储HDFS
    val s61= lines.filter(_.startsWith("61")).repartition(1)
    s61.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s61/" + dt1)
    val s62= lines.filter(_.startsWith("62")).repartition(1)
    s62.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s62/" + dt1+"/")
    val s65= lines.filter(_.startsWith("65")).repartition(1)
    s65.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s65/" + dt1+"/")
    val s66= lines.filter(_.startsWith("66")).repartition(1)
    s66.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s66/" + dt1+"/")


    //调用FTP接口存储      yaxin  yiyang

    s61.foreachRDD(rdd => {
        if (rdd.partitions.isEmpty) {
          logInfo(" No Data in this batchInterval --------")
        } else {
          val start_time = System.currentTimeMillis()
          rdd.mapPartitionsWithIndex((index, iter) => {
            val res2 = List[(String)]()
            process.intialize(ftpServer, ftpServerUsername, ftpServerPasswd, ftpServerPath, retryTime)
            process.writeAsPartitionToFTP(index, iter, 61, process61, cache, ftpServerPath, "/yaxin", "/yiyang", yaxin_codeMap.value, yiyang_codeMap.value)
            res2.iterator
          }).collect() //rdd.mapPartitionsWithIndex=>Iterator
          competeTime(start_time, "Processed 61_data write to FTP ")
        }
      })

    s62.foreachRDD(rdd => {
        if (rdd.partitions.isEmpty) {
          logInfo(" No Data in this batchInterval --------")
        } else {
          val start_time = System.currentTimeMillis()
          rdd.mapPartitionsWithIndex((index, iter) => {
            val res2 = List[(String)]()
            process.intialize(ftpServer, ftpServerUsername, ftpServerPasswd, ftpServerPath, retryTime)
            process.writeAsPartitionToFTP(index, iter, 62, process62, cache, ftpServerPath, "/yaxin", "/yiyang", yaxin_codeMap.value, yiyang_codeMap.value)
            res2.iterator
          }).collect() //rdd.mapPartitionsWithIndex=>Iterator
          competeTime(start_time, "Processed 62_data write to FTP ")
        }
      })

    s65.foreachRDD(rdd => {
        if (rdd.partitions.isEmpty) {
          logInfo(" No Data in this batchInterval --------")
        } else {
          val start_time = System.currentTimeMillis()
          rdd.mapPartitionsWithIndex((index, iter) => {
            val res2 = List[(String)]()
            process.intialize(ftpServer, ftpServerUsername, ftpServerPasswd, ftpServerPath, retryTime)
            process.writeAsPartitionToFTP(index, iter, 65, process65, cache, ftpServerPath, "/yaxin", "/yiyang", yaxin_codeMap.value, yiyang_codeMap.value)
            res2.iterator
          }).collect() //rdd.mapPartitionsWithIndex=>Iterator
          competeTime(start_time, "Processed 65_data write to FTP ")
        }
      })

    s66.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        logInfo(" No Data in this batchInterval --------")
      } else {
        val start_time = System.currentTimeMillis()
        rdd.mapPartitionsWithIndex((index, iter) => {
          val res2 = List[(String)]()
          process.intialize(ftpServer, ftpServerUsername, ftpServerPasswd, ftpServerPath, retryTime)
          process.writeAsPartitionToFTP(index, iter, 66, process66, cache, ftpServerPath, "/yaxin", "/yiyang", yaxin_codeMap.value, yiyang_codeMap.value)
          res2.iterator
        }).collect() //rdd.mapPartitionsWithIndex=>Iterator
        competeTime(start_time, "Processed 66_data write to FTP ")
      }
    })

    ssc
  }

  def main(args: Array[String]) {

    if (args.length != 3) {
      System.err.println("Usage:<conf> <host> <port> ")
      System.exit(1)
    }

    val checkpointDirectory ="/xinling/checkpoint_"+args(3)
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(args,checkpointDirectory)
      })
 //   val ssc= createContext(args,"")
    ssc.start()
    ssc.awaitTermination()

  }
}
