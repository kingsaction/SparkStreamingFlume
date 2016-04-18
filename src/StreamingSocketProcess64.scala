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

object StreamingSocketProcess64 extends Logging {
  private val process: ProcessSums = new ProcessSums()
  private val process64: Process = new Process(Array(11),50,1,18,19,"3g_address")

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

//    val masterUrl = PROPERTIES.getProperty("spark.master.url")
//    val socketHost = PROPERTIES.getProperty("socket.hostname")
//    val socketPort = PROPERTIES.getProperty("socket.port").toInt
    val socketHost=args(1)
    val socketPort=args(2)
    val batchInterval = Milliseconds(20000)
//    val ftpServer = PROPERTIES.getProperty("store.server")
//    val ftpServerUsername = PROPERTIES.getProperty("store.server.username")
//    val ftpServerPasswd = PROPERTIES.getProperty("store.server.passwd")
//    val ftpServerPath = PROPERTIES.getProperty("store.server.path")
//    val hdfsServerPath = PROPERTIES.getProperty("hdfs.server.path")
//    val cache = PROPERTIES.getProperty("spark.streaming.write-cache").toInt
//    val code_file_yaxin=PROPERTIES.getProperty("code.file.yaxin")
//    val code_file_yiyang=PROPERTIES.getProperty("code.file.yiyang")
//    val retryTime = 3

    val sparkConf = new SparkConf().setMaster("spark://e3basestorage1:7077").setAppName("StreamingSocketProcess64")
    sparkConf.set("spark.streaming.concurrentJobs", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.streaming.unpersist","false")
    val ssc = new StreamingContext(sparkConf, batchInterval)
     //ssc.checkpoint(checkpointDirectory)

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

    val s64 =  ssc.socketTextStream(socketHost, socketPort.toInt, StorageLevel.MEMORY_AND_DISK_SER_2)
//    s64.foreachRDD(rdd => {
    //      if(!rdd.partitions.isEmpty){
    //        logInfo("Received --------- " + rdd.count().toString + " --------- UnCleaned socket lines.")
    //      }else{
    //        logInfo("Received --------- 0 ---------- socket lines.")
    //      }
    //    })
    //
    //    val startTime1 = System.currentTimeMillis()
    //    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    //    val dt1 = sdf.format(new Date(startTime1))
    //    //源数据存储HDFS
    //    s64.saveAsTextFiles("/xinling/socket/source1/s65/"+dt1+"/"+startTime1+"/")
    s64.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        logInfo(" No 64_Data in this batchInterval --------")
      } else {
        val startTime = System.currentTimeMillis()
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val time = sdf.format(new Date(startTime))
        rdd.saveAsTextFile("hdfs://mycluster/xinling/socket/source1/s64/" + time +"/"+startTime)
        competeTime(startTime, "Processed 64_Data write to HDFS")
      }
    })

    ssc
  }

  def main(args: Array[String]) {

    if (args.length != 3) {
      System.err.println("Usage:<conf> <host> <port> ")
      System.exit(1)
    }

    val ssc= createContext(args,"")
    ssc.start()
    ssc.awaitTermination()

  }
}
