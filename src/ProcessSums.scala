import java.io._
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import org.apache.commons.net.ftp._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.Logging
import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.io.Source
import scala.util.{Random, Try}

import scala.util.{Random, Try}

final class ProcessSums extends Logging {
  private val md5: MD5 = new MD5()
  private val hadoopConf = new Configuration()
  private val fs: FileSystem = FileSystem.get(hadoopConf)
  private var host: String = ""
  private var uname: String = ""
  private var pwd: String = ""
  private var spath: String = ""
  private var retryTime: Int = 1
  def intialize(host: String, username: String, password: String, spath: String, retry: Int) = {
    this.host = host
    this.uname = username
    this.pwd = password
    this.spath = spath
    this.retryTime = retry
    //logInfo("host: "+host+"uname: "+uname+"pwd: "+pwd+"spath: "+spath+"retryTime: "+retry)
  }

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

  def readFromHDFS11(spath: String):  mutable.HashMap[String,String]  ={
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
    logInfo(spath+": -----size-------"+codeMap.size)
    codeMap
  }

  def readFromHDFS22(spath: String):  mutable.HashMap[String,String]  ={
    val data= Source.fromBytes(readFromHDFS(spath),"utf-8").getLines()
    var line=""
    val codeMap =new HashMap[String,String]
    while  (data.hasNext){
      line=data.next()
      val tmp = line.split("\t")
      if (tmp.length == 7) {
        codeMap(tmp(1) + "," + tmp(2)) = tmp(5)
      }
    }
    logInfo(spath+": -----size-------"+codeMap.size)
    codeMap
  }

  def writeToHDFS(spath: String, content: String): Try[Unit] = Try {
    val path = new Path(spath)
    try {
      if (!fs.exists(path)) {
        fs.createNewFile(path)
        if (fs.exists(path)) {
          logInfo("Successfully create file" + path + "\n")
        }
      }
    } catch {
      case e: IOException =>
        if (!fs.exists(path)) {
          logError("spark cann't create file " + path + " with IOException\n")
          logError("IOException : " + e.getMessage + "\n")
        }
    }
    if (!fs.exists(path)) {
      logInfo("spark cann't create file" + path + "\n")
    }
    val in = new ByteArrayInputStream(content.getBytes("UTF-8"))
    val out = fs.append(path)
    try {
      IOUtils.copyBytes(in, out, content.getBytes("UTF-8").length, true)
    }
    catch {
      case e: IOException => logError("Write hdfs IOException : " + e.getMessage + "\n")
    } finally {
      if (in != null) {
        in.close()
      }
      if (out != null) {
        out.close()
      }
    }
  }
  def setFtp(client: FTPClient): Unit = Try {
    client.setBufferSize(1024 * 1024)
    client.setControlEncoding("utf-8")
    //设置传输超时时间为60s
    client.setDataTimeout(240000)
    //连接超时为60s
    client.setConnectTimeout(20000)
    client.setSoTimeout(240000)
    //设置请求模式，否则会报错
    client.enterLocalPassiveMode()
    client.setFileTransferMode(FTP.STREAM_TRANSFER_MODE)
    //设置文件类型（二进制)
    client.setFileType(FTP.BINARY_FILE_TYPE)
  }
  def exist(client: FTPClient, path: String, subpath: String): Boolean = {
    var flag = false
    client.enterLocalPassiveMode()
    val ftpFiles = client.listFiles(path)
    if (ftpFiles.length > 0) {
      for (i <- 0 until ftpFiles.length if !flag) {
        logInfo("exist:" + ftpFiles(i).getName + "  " + ftpFiles(i).isDirectory + "\n")
        if (ftpFiles(i).isDirectory && ftpFiles(i).getName.equalsIgnoreCase(subpath)) {
          flag = true
        }
      }
    }
    flag
  }

  def FTPWrite(client: FTPClient, path: String,  content: String,index: Int): Boolean = {
    val startTime = System.currentTimeMillis()
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val time = sdf.format(new Date(startTime))
    val date= new Date(startTime)
    val subpath = path + "/" + time
    val ran = new Random().nextInt(10000)
    val ran1 = new Random().nextInt(10000) //16-1453191141370-815-14.txt@tmp
    val fname =subpath+ "/" +date.getHours.toString+"-"+startTime+"-"+ran+"-"+ran1
    val istream = new BufferedInputStream(new ByteArrayInputStream(content.getBytes("UTF-8")))
    var flag = false
    var count = 0
    var flag_ftp = false
    while (!flag && count < retryTime) {
      try {
        if (!client.isConnected) {
          count = count + 1
          logInfo("FTP is not connected, and retry .")
          client.connect(host, 21)
          client.login(uname, pwd)
          logInfo("Connected to " + host + ".")
          logInfo("reply code: " + client.getReplyString)
          val reply = client.getReplyCode
          if (!FTPReply.isPositiveCompletion(reply)) {
            client.disconnect()
            logError("FTP server refused connection.")
          }
          setFtp(client)
        }

        if (!exist(client, path, time)) {
          client.makeDirectory(subpath)
        }
        logInfo("upload file" + fname +" to FTP")
        flag = client.storeUniqueFile(fname, istream)

        if (flag) {
          logInfo("successfully upload file" + fname + ".")

//          flag_ftp=client.rename(fname, fname.split("@")(0))
//          if(flag_ftp){
//            logInfo("successfully rename file" + fname)
//          }

        }
      } catch {
        case e: Exception =>
          count = count + 1
          if (client.isConnected) {
            try {
              client.disconnect()
            } catch {
              case ioe: IOException =>
                logError("disconnect ftp client IOException: " + ioe.getMessage + "\n")
            }
          }
          logError("循环处理" + count + flag + "\n")
          logError("FTPWrite Exception Message: " + e.getMessage + "\n")
          logError("printStackTrace: " + e.printStackTrace() + "\n")
      }
    }
    if (istream != null) {
      istream.close()
    }
    flag
  }
  //按分区逐条缓存写入hdfs+FTP到远端主机，异常未写入时写到hdfs
  //  def writeAsPartitionToHDFS( iter: Iterator[(String)], fil_pre: String, cache: Int): Try[Unit] = Try {
  //    val fileWrite_prefix = fil_pre
  //   val date = new Date()
  //    var record = iter.next() //当前记录(file，value)
  //    var content = record + "\n" //当前记录要写入的value
  //    var sum = content //累计要写入的value
  //    var cache_record = 1 //累计cache条数，计数器
  //    val startTime = System.currentTimeMillis()
  //    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  //    val dt = sdf.format(new Date(startTime))
  //    val fileWrite_sufix = dt + "/" + date.getHours  //当前写入路径后缀
  //    while (iter.hasNext) {
  //      record = iter.next() //取下一条记录
  //      content = record + "\n" //获取当前记录要写入的value
  //      sum = content + sum
  //      cache_record = cache_record + 1 //更新同文件写入计数器
  //      //缓存达到，写入，清理sum和计数器
  //            if (cache_record == cache && !sum.isEmpty) {
  //        writeToHDFS(fileWrite_prefix + fileWrite_sufix, sum)
  //        sum = ""
  //        cache_record = 0
  //      }
  //      if (!sum.isEmpty) {
  //        writeToHDFS(fileWrite_prefix + fileWrite_sufix, sum)
  //      }
  //    }
  //  }

  // 按分区逐条缓存写入hdfs+FTP到远端主机，异常未写入时写到hdfs          //,map:mutable.HashMap[String,String]

  def writeAsPartitionToHDFS( index:Int,iter: Iterator[(String)], fil_pre: String, cache: Int): Try[Unit] = Try {
    var sum60=""
    var sum67=""
    var sum68=""
    var sum69=""
    var sum70=""
    var c60=0
    var c67=0
    var c68=0
    var c69=0
    var c70=0
    var record=""

    val startTime = System.currentTimeMillis()
    val ran=new Random().nextInt(1000)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val dt = sdf.format(new Date(startTime))
    val date= new Date(startTime)
    val fileWrite_sufix = "/"+dt+"/"+date.getHours.toString+"-"+startTime.toString+"-"+ran//当前写入路径后缀
    //val fileWrite_sufix = "/"+dt+"/"+date.getHours.toString+"-"+index+"-"+ran+"-"+startTime.toString   //当前写入路径后缀
    //val fileWrite_sufix = "/"+dt+"/"+date.getHours.toString+"-"+index  //当前写入路径后缀
    while (iter.hasNext) {
      record = iter.next() + "\n"
      if( record.startsWith("60")){
        sum60 = record + sum60
        c60=c60+1
      }

      if( record.startsWith("67")){
        sum67 = record + sum67
        c67=c67+1
      }

      if( record.startsWith("68")){
        sum68 = record + sum68
        c68=c68+1
      }

      if( record.startsWith("69")){
        sum69 = record + sum69
        c69=c69+1
      }

      if( record.startsWith("70")){
        sum70 = record + sum70
        c70=c70+1
      }


      if (c60 == cache && !sum60.isEmpty) {
          writeToHDFS(fil_pre+"/s60" + fileWrite_sufix, sum60)
          sum60 = ""
          c60 = 0
        }

      if (c67 == cache && !sum67.isEmpty) {
        writeToHDFS(fil_pre+"/s67" + fileWrite_sufix, sum67)
        sum67 = ""
        c67 = 0
      }
      if (c68 == cache && !sum68.isEmpty) {
        writeToHDFS(fil_pre+"/s68" + fileWrite_sufix, sum68)
        sum68 = ""
        c68 = 0
      }
      if (c69 == cache && !sum69.isEmpty) {
        writeToHDFS(fil_pre+"/s69" + fileWrite_sufix, sum69)
        sum69 = ""
        c69 = 0
      }
      if (c70 == cache && !sum70.isEmpty) {
        writeToHDFS(fil_pre+"/s70" + fileWrite_sufix, sum70)
        sum70 = ""
        c70 = 0
      }
    }


    if (!sum60.isEmpty) {
      writeToHDFS(fil_pre+"/s60" + fileWrite_sufix, sum60)
    }

    if (!sum67.isEmpty) {
      writeToHDFS(fil_pre+"/s67" + fileWrite_sufix, sum67)
    }
    if (!sum68.isEmpty) {
      writeToHDFS(fil_pre+"/s68" + fileWrite_sufix, sum68)
    }
    if (!sum69.isEmpty) {
      writeToHDFS(fil_pre+"/s69" + fileWrite_sufix, sum69)
    }
    if (!sum70.isEmpty) {
      writeToHDFS(fil_pre+"/s70" + fileWrite_sufix, sum70)
    }
  }


  def writeAsPartitionToFTP( index: Int,iter: Iterator[(String)],tab:Int,process: Process, cache: Int, ftpServerPath: String,partner1:String,partner2:String,map1:mutable.HashMap[String,String],map2:mutable.HashMap[String,String]): Try[Unit] = Try {

    var sum1=""
    var sum2=""
    var count_sum1=0
    var count_sum2=0
    var record=""
    val client = new FTPClient()

    val startTime = System.currentTimeMillis()
    val ran1=new Random().nextInt(1000)
    val ran2=new Random().nextInt(5)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val dt = sdf.format(new Date(startTime))
    val date= new Date(startTime)
    val fileWrite_sufix = "/"+dt+"/"+date.getHours.toString+"-"+startTime+"-"+ran1+"-"+ran2 //当前写入路径后缀

    val ftpPath1=ftpServerPath+partner1
    val hdfsPath1="/xinling/socket/deal"+partner1

    val ftpPath2=ftpServerPath+partner2
    val hdfsPath2="/xinling/socket/deal"+partner2
    try {
      client.connect(host, 21)
      client.login(uname, pwd)
      logInfo("first Connected to " + host + ".")
      logInfo("first connection reply code" + client.getReplyString)
      val reply = client.getReplyCode
      if (!FTPReply.isPositiveCompletion(reply)) {
        client.disconnect()
        logError("first FTP server refused connection.")
      }
      setFtp(client)

      while (iter.hasNext) {


//          sum1 = processTable(record,map1,process) + sum1
//          sum2 = processTable(record,map2,process) + sum2

            record = iter.next()+"\n"
            sum1 = record+ sum1
            sum2 =  record+ sum2
          count_sum1=count_sum1+1
          count_sum2=count_sum2+1

        if (count_sum1 == cache && !sum1.isEmpty) {
            FTPWrite(client, ftpPath1+"/s"+tab, sum1, index)
            writeToHDFS(hdfsPath1+"/s"+tab+ fileWrite_sufix, sum1)
            sum1 = ""
            count_sum1 = 0
        }

        if (count_sum2 == cache && !sum2.isEmpty) {
            FTPWrite(client, ftpPath2+"/s"+tab, sum2, index)
            writeToHDFS(hdfsPath2+"/s"+tab+ fileWrite_sufix, sum2)
            sum2 = ""
            count_sum2 = 0
        }

      }

      if (!sum1.isEmpty) {
        FTPWrite(client, ftpPath1+"/s"+tab, sum1, index)
        writeToHDFS(hdfsPath1+"/s"+tab+ fileWrite_sufix, sum1)
      }
      if (!sum2.isEmpty) {
        FTPWrite(client, ftpPath2+"/s"+tab, sum2, index)
        writeToHDFS(hdfsPath2+"/s"+tab+ fileWrite_sufix, sum2)
      }

    } catch {
      case e: IOException =>
        logError("partition first connect ftp exception : " + e.getMessage + "\n")
    } finally {
      if (client.isConnected) {
        try {
          client.disconnect()
        } catch {
          case ioe: IOException =>
            logError("disconnect ftp client IOException: " + ioe.getMessage + "\n")
        }
      }
    }
  }


//  //解析原始数据表规则
//  def  processTable(line: String,map:mutable.HashMap[String,String],process: Process): String = {
//    var data = ""
//    val result = line.split(",")
//    var called = ""
//
//    val phone=process.array
//    val result_length=process.line_length
//    val start_time=process.starttime
//    val lac=process.lac
//    val ci=process.ci
//    val add_index=process.sufix_flag
//
//    //过滤掉脏数据
//     if(result.length==result_length) {
//    for (m <- 0 until phone.length) {
//      called = called + md5.toDigest(result(phone(m))) + ","
//    }
//    val la_ci = result(lac) + "," + result(ci) //拼接lac ci
//
//    val flag = map.getOrElse(la_ci, "") //没有匹配上的就设置为空串
//
//    if (flag != "") {
//      //手机号，开始时间，lac_ci标签，新增字段
//      data = called + result(start_time) + "," + flag + "," + add_index + "\n"
//    }
//
//    }
//    data
//  }

}
