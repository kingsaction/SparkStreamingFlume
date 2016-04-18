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

object StreamingSocketProcess62 extends Logging {
  private val process: ProcessSums = new ProcessSums()
  private val process63: Process = new Process(Array(11),50,1,18,19,"2g_address")

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

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("StreamingSocketProcess63").set("spark.driver.allowMultipleContexts", "true")
    sparkConf.set("spark.streaming.concurrentJobs", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.set("spark.cleaner.ttl", "12000")
    //sparkConf.set("spark.default.parallelism","400")
    sparkConf.set("spark.storage.memoryFraction","0.5")
    sparkConf.set("spark.streaming.unpersist","true")
    val ssc = new StreamingContext(sparkConf, batchInterval)
     //ssc.checkpoint(checkpointDirectory)

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

    val s63 =  ssc.socketTextStream(socketHost, socketPort.toInt, StorageLevel.MEMORY_AND_DISK_SER_2)

    s63.foreachRDD(rdd => {
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
    s63.saveAsTextFiles("hdfs://sparkCluster/xinling/socket/source/s63/"+dt1+"/")

    //调用FTP接口存储      yaxin  yiyang

    s63.foreachRDD(rdd => {
        if (rdd.partitions.isEmpty) {
          logInfo(" No Data in this batchInterval --------")
        } else {
          val start_time = System.currentTimeMillis()
          rdd.mapPartitionsWithIndex((index, iter) => {
            val res2 = List[(String)]()
            process.intialize(ftpServer, ftpServerUsername, ftpServerPasswd, ftpServerPath, retryTime)
            process.writeAsPartitionToFTP(index, iter, 63, process63, cache, ftpServerPath, "/yaxin", "/yiyang", yaxin_codeMap.value, yiyang_codeMap.value)
            res2.iterator
          }).collect() //rdd.mapPartitionsWithIndex=>Iterator
          competeTime(start_time, "Processed 63_data write to FTP ")
        }
      })

    ssc
  }

  def main(args: Array[String]) {

    if (args.length != 3) {
      System.err.println("Usage:<conf> <host> <port> ")
      System.exit(1)
    }

//    val checkpointDirectory ="/xinling/checkpoint_"+args(3)
//    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
//      () => {
//        createContext(args,checkpointDirectory)
//      })
    val ssc= createContext(args,"")
    ssc.start()
    ssc.awaitTermination()

  }
}
