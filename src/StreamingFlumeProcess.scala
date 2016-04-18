import java.util.{Date, Properties}
import java.net.InetSocketAddress
import java.io._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel

import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ListBuffer
import java.text.SimpleDateFormat

/**
 * spark-submit  --class com.sitech.streaming.StreamingFlumeProcess --driver-memory 2g --executor-memory 1g --total-executor-cores 10 spark-streaming-5.3.1.jar ./conf
 * spark-submit  --class com.sitech.streaming.StreamingFlumeProcess --driver-memory 1g --executor-memory 1g spark-streaming-5.3.1.jar ./conf
 * spark-submit --deploy-mode cluster --supervise --class com.sitech.streaming.StreamingFlumeProcess --total-executor-cores 5 hdfs://drmcluster/streaming/spark-streaming-5.3.1.jar hdfs://drmcluster/streaming/conf
 *
 */

object StreamingFlumeProcess extends Logging {
  private val process: ProcessSum = new ProcessSum1()    
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def competeTime(Stime: Long, prefix: String = ""): Unit = {
    val end_time = System.currentTimeMillis
    val cost_time = end_time.-(Stime)
    logInfo(prefix + " cost time :" + cost_time.toString + "----------------")
  }

  def createContext(args: Array[String] , checkpointDirectory: String) = {

    //  initialization
    val in = new FileInputStream(args(0))

    //    val path = new Path(args(0))
    //    val hadoopConf = new Configuration()
    //    val fs: FileSystem = FileSystem.get(hadoopConf)
    //    val in = fs.open(path)

    val PROPERTIES = new Properties()
    PROPERTIES.load(in)


    val masterUrl = PROPERTIES.getProperty("spark.master.url")
    val flumeAddres = PROPERTIES.getProperty("flume.address").split(",")
//    val flumeHost = PROPERTIES.getProperty("flume.hostname").split(",")
//    val flumePort = PROPERTIES.getProperty("flume.port").split(",")
    val batchInterval = Milliseconds(PROPERTIES.getProperty("spark.streaming.batchInterval").toInt)
    val ftpServer = PROPERTIES.getProperty("store.server")
    val ftpServerUsername = PROPERTIES.getProperty("store.server.username")
    val ftpServerPasswd = PROPERTIES.getProperty("store.server.passwd")
    val ftpServerPath = PROPERTIES.getProperty("store.server.path")
    val maxBatchSize = PROPERTIES.getProperty("spark.streaming.maxBatchSize").toInt
    val parallelism = PROPERTIES.getProperty("spark.streaming.parallelism").toInt
    val cache = PROPERTIES.getProperty("spark.streaming.write-cache").toInt    
    val fileWrite_prefix = PROPERTIES.getProperty("spark.streaming.fileWrite_prefix")
    val str_renfix = PROPERTIES.getProperty("spark.streaming.str_renfix")
    val currJob = PROPERTIES.getProperty("spark.streaming.concurrentJobs")
    val retryTime = 3


    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("StreamingFlumeProcess")
//    sparkConf.set("spark.cleaner.ttl",3600)
    //sparkConf.set("spark.streaming.concurrentJobs", "3")
    sparkConf.set("spark.streaming.concurrentJobs", currJob)
    //    sparkConf.getBoolean("spark.streaming.receiver.writeAheadLog.enable",true)
    //    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable",true)


    val ssc = new StreamingContext(sparkConf, batchInterval)
    ssc.checkpoint(checkpointDirectory)

    //create stream
    val storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2

//    val addresses = Seq(new InetSocketAddress(flumeHost, flumePort))
//    val stream = FlumeUtils.createPollingStream(ssc, addresses, storageLevel, maxBatchSize, parallelism)

//    val address1 = new InetSocketAddress("172.16.14.41", 11111)
//    val address2 = new InetSocketAddress("172.16.14.42", 22222)
//    val addresses = Seq(address2, address1)
//    val stream = FlumeUtils.createPollingStream(ssc, addresses, storageLevel, maxBatchSize, parallelism)


    val buf = new ListBuffer[ReceiverInputDStream[SparkFlumeEvent]]
    for(i <- 0 to flumeAddres.length-1)
    {
      val adds = flumeAddres(i).split(":")
      if(adds.length == 2){
        val one_address = Seq(new InetSocketAddress(adds(0), adds(1).toInt))
        val one_stream = FlumeUtils.createPollingStream(ssc, one_address, storageLevel, maxBatchSize, parallelism)
        buf += one_stream
        logInfo("Flume Server : " + flumeAddres(i))
      }else{
        logInfo("Flume Server Information  is not complete : " + flumeAddres(i))
      }
    }
    val stream = ssc.union(buf.toSeq)



    //received streaming data
    stream.foreachRDD(rdd => {
      if(!rdd.partitions.isEmpty){
        logInfo("Received --------- " + rdd.count().toString + " ---------- flume events.")
      }else{
        logInfo("Received --------- 0 ---------- flume events.")
      }
    })


    //解析流数据,格式:(filename,record)
    val fileWrite = stream.map(t => {
      val value = new String(t.event.getBody.array())
      val filename = t.event.getHeaders.get("flieName")
      (filename.toString, value)
    })

//    //处理后保存数据到hdfs
//    val startTime = System.currentTimeMillis()
//    val sdf = new SimpleDateFormat("yyyyMMdd")
//    val time = sdf.format(new Date(startTime))    
//    fileWrite.map(t => {
//               // "add_" + t._2
//              logInfo("zzzprevalue"+ t._2)
//              var contentRen = DesenPross.strRen(t._2, 0).get
//               logInfo("prevalue"+ t._2)
//              logInfo("value:" + contentRen)
//               contentRen
//              }).saveAsTextFiles("/duxm/streaming/sav/"+time)    
 fileWrite.foreachRDD(rdd => {
          if (rdd.partitions.isEmpty) {            
            logInfo(" No Data in this batchInterval --------")
          } else {
            val startTime = System.currentTimeMillis()
            val sdf = new SimpleDateFormat("yyyyMMdd")
            val time = sdf.format(new Date(startTime))
            //val fileWrite_prefix = "/gninfo/streaming/"
            val res=rdd.map(t => {
             // val value = "add_" + t._2
             // logInfo("zzzprevalue"+ t._2)
             var contentRen = DesenPross.strRen(t._2, 0).get
                 contentRen = DesenPross.strRen(contentRen, 1).get
                 contentRen = DesenPross.strRen(contentRen, 2).get
                 contentRen = DesenPross.strRen(contentRen, 7).get
             // var contentRen = DesenPross.strRen(t._2, 0).get
             //  logInfo("prevalue"+ t._2)
             // logInfo("value:" + contentRen)
               contentRen
            })
            res.saveAsTextFile(fileWrite_prefix + time +"/"+ startTime)
            competeTime(startTime, "Processed data write to HDFS")
          }
        }) 


    //按照文件名保存处理后的数据ftp
    fileWrite.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        logInfo(" No Data in this batchInterval --------")
      } else {
        val start_time = System.currentTimeMillis()
        val fileWrite_prefix1 = "/duxm/streaming/res/"
        val res=rdd.map(t => {
        var contentRen = DesenPross.strRen(t._2, 0).get
         contentRen = DesenPross.strRen(contentRen, 1).get
         contentRen = DesenPross.strRen(contentRen, 2).get
         contentRen = DesenPross.strRen(contentRen, 7).get
        //var contentRen = DesenPross.strRen(t._2, 0).get
          (t._1, contentRen)
        })
        res.mapPartitionsWithIndex((index, iter) => {
          val res2 = List[(Int, String, String)]()

          process.intialize(ftpServer, ftpServerUsername, ftpServerPasswd,ftpServerPath,retryTime)

         // process.writeAsPartitionToFTP(index, iter, fileWrite_prefix1, cache, ftpServerPath)
           process.writeAsPartitionToFTPUnion(index, iter, fileWrite_prefix1, cache, ftpServerPath)
          res2.iterator
        }).collect() //rdd.mapPartitionsWithIndex=>Iterator

        competeTime(start_time, "Processed data write to FTPserver")
      }
    }) //foreachRDD



//    //按照文件名保存处理后的数据hdfs
//    fileWrite.foreachRDD(rdd => {
//      if (rdd.partitions.isEmpty) {
//        logInfo(" No Data in this batchInterval --------")
//      } else {
//        val start_time = System.currentTimeMillis()
//        val fileWrite_prefix1 = "/duxm/streaming/res/"
//        val res=rdd.map(t => {
//          val value = "add_" + t._2
//          (t._1, value)
//        })
//        res.mapPartitionsWithIndex((index, iter) => {
//          val res2 = List[(Int, String, String)]()
//          process.writeAsPartitionToHDFS(index, iter, fileWrite_prefix1, cache)
//          res2.iterator
//        }).collect() //rdd.mapPartitionsWithIndex=>Iterator
//
//        competeTime(start_time, "Processed data write to HDFS")
//      }
//    }) //foreachRDD


//    //按照文件名保存处理后的数据hdfs+ftp
//    fileWrite.foreachRDD(rdd => {
//      if (rdd.partitions.isEmpty) {
//        logInfo(" No Data in this batchInterval --------")
//      } else {
//        val start_time = System.currentTimeMillis()
//        val fileWrite_prefix1 = "/duxm/streaming/res/"
//        rdd.map(t => {
//          val value = "add_" + t._2
//          (t._1, value)
//        }).mapPartitionsWithIndex((index, iter) => {
//          val res2 = List[(Int, String, String)]()
//
//          process.intialize(ftpServer, ftpServerUsername, ftpServerPasswd,ftpServerPath,retryTime)
//
//          process.writeAsPartitionToHDFSAndFTP(index, iter, fileWrite_prefix1, cache, ftpServerPath)
//
//          res2.iterator
//        }).collect() //rdd.mapPartitionsWithIndex=>Iterator
//        competeTime(start_time, "Processed data write to hdfs and FTP")
//
//      }
//    }) //foreachRDD

    ssc
  }

  def main(args: Array[String]) {

   // val checkpointDirectory = "hdfs://sparkCluster/duxm/streaming/checkpoint"
     val checkpointDirectory = args(1)
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(args,checkpointDirectory)
      })

    ssc.start()
    ssc.awaitTermination()

  }
}


