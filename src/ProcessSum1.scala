import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils


import scala.util.Try
import java.io.{IOException, BufferedInputStream, ByteArrayInputStream}
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Random
import org.apache.commons.net.ftp._
import org.apache.spark.Logging

/**
 * Created by IBM on 15-10-28.
 */
final class ProcessSum1 extends Logging{
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
  }


  def writeToHDFS(spath: String, content: String): Try[Unit] = Try {
//    val hadoopConf = new Configuration()
//    val fs = FileSystem.get(hadoopConf)

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

  def setFtp(client: FTPClient): Unit = Try{
    client.setBufferSize(1024 * 1024)
    client.setControlEncoding("utf-8")

    //设置传输超时时间为60s
    client.setDataTimeout(240000)
    //连接超时为60s
    client.setConnectTimeout(10000)
    client.setSoTimeout(240000)

    //设置请求模式，否则会报错
    client.enterLocalPassiveMode()
    client.setFileTransferMode(FTP.STREAM_TRANSFER_MODE)

    //设置文件类型（二进制)
    client.setFileType(FTP.BINARY_FILE_TYPE)
  }

  def exist(client: FTPClient,path: String, subpath: String): Boolean = {
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


  def FTPWrite(client:FTPClient, path: String, filename: String, index: Int, content: String): Boolean = {
    val startTime = System.currentTimeMillis()
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val time = sdf.format(new Date(startTime))

    val subpath = path + "/" + time
    val ran = new Random().nextInt(100)
    //val fname = subpath + "/" + filename + "-" + System.currentTimeMillis().toString + "-" + index + "-" + ran
    val fname = subpath + "/" + filename + "-" + System.currentTimeMillis().toString + "-" + index + "-" + ran+"@tmp"
    //    val istream = new ByteArrayInputStream(content.getBytes("UTF-8"))
    val istream = new BufferedInputStream(new ByteArrayInputStream(content.getBytes("UTF-8")))

    var flag = false
    var count = 0
    var flag_ftp =false 
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
        logInfo("upload file" + fname + ".")
        flag = client.storeFile(fname, istream)
        
        if (flag) {
          logInfo("successfully upload file" + fname + ".")
          flag_ftp=client.rename(fname, fname.split("@")(0))
           if(flag_ftp){
             logInfo("successfully rename file" + fname)
           }
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

  /*
  ftp上传失败时，写入hdfs
   */
  def FTPErrorWriteToHDFS(dfspath: String, filename: String, index: Int, content: String): Try[Unit] = Try {
    val startTime = System.currentTimeMillis()
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val time = sdf.format(new Date(startTime))

    val subpath = dfspath + "/" + time
    val ran = new Random().nextInt(100)
    val fname = subpath + "/" + filename + "-" + System.currentTimeMillis().toString + "-" + index + "-" + ran

    logInfo("dfspath:" + dfspath + "\n")
    logInfo("subpath:" + subpath + "\n")
    logInfo("fname:" + fname + "\n")


    val path = new Path(fname)

    try {
      val currentPath = new Path(subpath)
      if (!fs.exists(currentPath)) {
        fs.mkdirs(currentPath)
      }
      if (!fs.exists(path)) {
        fs.createNewFile(path)
      }
    } catch {
      case e: IOException =>
        logError("HDFS createNewFile IOException : " + e.getMessage + "\n")
    }

    val istream = new ByteArrayInputStream(content.getBytes("UTF-8"))
    val ostream = fs.append(path)

    try {

      if (fs.exists(path)) {

        IOUtils.copyBytes(istream, ostream, content.getBytes("UTF-8").length, true)
      } else {

        logError("------------ftp异常写DFS数据，未成功，数据丢失-------------\n")
      }
    } catch {
      case e: IOException =>
        logError("HDFS write IOException : " + e.getMessage + "\n")
    } finally {
      if (istream != null) {
        istream.close()
      }
      if (ostream != null) {
        ostream.close()
      }
    }
  }



  //按分区逐条缓存写入hdfs+FTP到远端主机，异常未写入时写到hdfs
  def writeAsPartitionToHDFSAndFTP(index: Int, iter: Iterator[(String, String)], fil_pre: String, cache: Int, ftpServerPath: String): Try[Unit] = Try {
    val fileWrite_prefix = fil_pre
    val time = System.currentTimeMillis()
    var record = iter.next() //当前记录(file，value)
    var last = record //上一条记录(file，value)
    var content = record._2 + "\n" //当前记录要写入的value
    var sum = content //累计要写入的value
    var cache_record = 1 //累计cache条数，计数器
    var fileWrite_sufix = record._1 + "/" + time.toString + "-" + index //当前写入路径后缀
    val client = new FTPClient()

    try{
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
      record = iter.next() //取下一条记录
      content = record._2 + "\n" //获取当前记录要写入的value
      if (record._1 == last._1) {
        //同一个文件写入
        sum = content + sum
        cache_record = cache_record + 1 //更新同文件写入计数器
        //缓存达到，写入，清理sum和计数器
        if (cache_record == cache && !sum.isEmpty)
        {
          writeToHDFS(fileWrite_prefix + fileWrite_sufix, sum)
          if (!FTPWrite(client, ftpServerPath, record._1, index, sum)) {
            logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
            FTPErrorWriteToHDFS(ftpServerPath, record._1, index, sum)
          }
          sum = ""
          cache_record = 0
        }

      } else {
        //不同文件，之前的写入，之后的进入下一个文件的循环，计数重新开始
        if (!sum.isEmpty) {
          writeToHDFS(fileWrite_prefix + fileWrite_sufix, sum)
          if (!FTPWrite(client, ftpServerPath, last._1, index, sum)) {
            logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
            FTPErrorWriteToHDFS(ftpServerPath, last._1, index, sum)
          }
        }
        sum = content
        cache_record = 1
        fileWrite_sufix = record._1 + "/" + time.toString + "-" + index
      }
      last = record //更新last记录，循环读取
    }
    if (!sum.isEmpty) {
      writeToHDFS(fileWrite_prefix + fileWrite_sufix, sum)
      if (!FTPWrite(client, ftpServerPath, record._1, index, sum)) {
        logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
        FTPErrorWriteToHDFS(ftpServerPath, record._1, index, sum)
      }
    }

    }catch {
      case e: IOException =>
        logError("partition first connect ftp exception : " + e.getMessage + "\n")
    }finally{
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


  //按分区逐条缓存写入hdfs
  def writeAsPartitionToHDFS(index: Int, iter: Iterator[(String, String)], fil_pre: String, cache: Int): Try[Unit] = Try {
    val fileWrite_prefix = fil_pre
    val time = System.currentTimeMillis()
    var record = iter.next() //当前记录(file，value)
    var last = record //上一条记录(file，value)
    var content = record._2 + "\n" //当前记录要写入的value
    var sum = content //累计要写入的value
    var cache_record = 1 //累计cache条数，计数器
    var fileWrite_sufix = record._1 + "/" + time.toString + "-" + index //当前写入路径后缀

      while (iter.hasNext) {
        record = iter.next() //取下一条记录
        content = record._2 + "\n" //获取当前记录要写入的value
        if (record._1 == last._1) {
          //同一个文件写入
          sum = content + sum
          cache_record = cache_record + 1 //更新同文件写入计数器
          //缓存达到，写入，清理sum和计数器
          if (cache_record == cache && !sum.isEmpty)
          {
            writeToHDFS(fileWrite_prefix + fileWrite_sufix, sum)
            sum = ""
            cache_record = 0
          }

        } else {
          //不同文件，之前的写入，之后的进入下一个文件的循环，计数重新开始
          if (!sum.isEmpty) {
            writeToHDFS(fileWrite_prefix + fileWrite_sufix, sum)
          }
          sum = content
          cache_record = 1
          fileWrite_sufix = record._1 + "/" + time.toString + "-" + index
        }
        last = record //更新last记录，循环读取
      }
      if (!sum.isEmpty) {
        writeToHDFS(fileWrite_prefix + fileWrite_sufix, sum)
      }

  }


  //按分区逐条缓存FTP到远端主机，异常未写入时写到hdfs
  def writeAsPartitionToFTP(index: Int, iter: Iterator[(String, String)], fil_pre: String, cache: Int, ftpServerPath: String): Try[Unit] = Try {
    val time = System.currentTimeMillis()
    var record = iter.next() //当前记录(file，value)
    var last = record //上一条记录(file，value)
    var content = record._2 + "\n" //当前记录要写入的value
    var sum = content //累计要写入的value
    var cache_record = 1 //累计cache条数，计数器
    var fileWrite_sufix = record._1 + "/" + time.toString + "-" + index //当前写入路径后缀
    val client = new FTPClient()

    try{
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
        record = iter.next() //取下一条记录
        content = record._2 + "\n" //获取当前记录要写入的value
        if (record._1 == last._1) {
          //同一个文件写入
          sum = content + sum
          cache_record = cache_record + 1 //更新同文件写入计数器
          //缓存达到，写入，清理sum和计数器
          if (cache_record == cache && !sum.isEmpty)
          {
            if (!FTPWrite(client, ftpServerPath, record._1, index, sum)) {
              logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
              FTPErrorWriteToHDFS(ftpServerPath, record._1, index, sum)
            }
            sum = ""
            cache_record = 0
          }

        } else {
          //不同文件，之前的写入，之后的进入下一个文件的循环，计数重新开始
          if (!sum.isEmpty) {
            if (!FTPWrite(client, ftpServerPath, last._1, index, sum)) {
              logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
              FTPErrorWriteToHDFS(ftpServerPath, last._1, index, sum)
            }
          }
          sum = content
          cache_record = 1
          fileWrite_sufix = record._1 + "/" + time.toString + "-" + index
        }
        last = record //更新last记录，循环读取
      }
      if (!sum.isEmpty) {
        if (!FTPWrite(client, ftpServerPath, record._1, index, sum)) {
          logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
          FTPErrorWriteToHDFS(ftpServerPath, record._1, index, sum)
        }
      }

    }catch {
      case e: IOException =>
        logError("partition first connect ftp exception : " + e.getMessage + "\n")
    }finally{
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
  
  /*
   *合并文件，不是1000条写一次
   *  一个分区写一次
   */
  def writeAsPartitionToFTPUnion(index: Int, iter: Iterator[(String, String)], fil_pre: String, cache: Int, ftpServerPath: String): Try[Unit] = Try {
    val time = System.currentTimeMillis()
    var record = iter.next() //当前记录(file，value)
    var last = record //上一条记录(file，value)
    var content = record._2 + "\n" //当前记录要写入的value
    var sum = content //累计要写入的value
    var cache_record = 1 //累计cache条数，计数器
    var fileWrite_sufix = record._1 + "/" + time.toString + "-" + index //当前写入路径后缀
    val client = new FTPClient()

    try{
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
        record = iter.next() //取下一条记录
        content = record._2 + "\n" //获取当前记录要写入的value
        if (record._1 == last._1) {
          //同一个文件写入
          sum = content + sum
          cache_record = cache_record + 1 //更新同文件写入计数器
          //缓存达到，写入，清理sum和计数器
//          if (cache_record == cache && !sum.isEmpty)
//          {
//            if (!FTPWrite(client, ftpServerPath, record._1, index, sum)) {
//              logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
//              FTPErrorWriteToHDFS(ftpServerPath, record._1, index, sum)
//            }
//            sum = ""
//            cache_record = 0
//          }

        } else {
          //不同文件，之前的写入，之后的进入下一个文件的循环，计数重新开始
          if (!sum.isEmpty) {
            if (!FTPWrite(client, ftpServerPath, last._1, index, sum)) {
              logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
              FTPErrorWriteToHDFS(ftpServerPath, last._1, index, sum)
            }
          }
          sum = content
          cache_record = 1
          fileWrite_sufix = record._1 + "/" + time.toString + "-" + index
        }
        last = record //更新last记录，循环读取
      }
      if (!sum.isEmpty) {
        if (!FTPWrite(client, ftpServerPath, record._1, index, sum)) {
          logInfo("------------写ftp数据，未成功，数据丢失,写入HDFS-------------" + ftpServerPath + "\n")
          FTPErrorWriteToHDFS(ftpServerPath, record._1, index, sum)
        }
      }

    }catch {
      case e: IOException =>
        logError("partition first connect ftp exception : " + e.getMessage + "\n")
    }finally{
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



}
