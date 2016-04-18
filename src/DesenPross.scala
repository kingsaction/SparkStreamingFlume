import java.io._
import java.net.SocketException

import org.apache.commons.net.ftp._

import scala.util.Try
import com.sitech.sparkstreaming.util.AgiString
import scala.collection.immutable.HashMap
import com.sitech.sparkstreaming.util.MD5;

//object DesenPross extends App {
object DesenPross {

  private val ftpclient: FTPClient = new FTPClient
  private val pre_flag = "\\|"

  /*
   * Getter
   */
  def getFtpclient = ftpclient

  /**
   * Md5加密字符串老
   */
  def md5Covert_old(sourceText: String): Try[String] = Try {
    val resultText = AgiString.md5(sourceText)
    resultText
  }
  
  /**
   * Md5加密字符串新
   */
  def md5Covert(sourceText: String): Try[String] = Try {
    val AgiString = new MD5()
    val resultText = AgiString.toDigest(sourceText)
    resultText
  }
  
  /**
   * 返回map对象的value
   * 
   */
  def ioInput(key: String,map: Map[String,String]): Try[String] = Try {
    //var map:Map[String,String] = HashMap()    
    var retValue =  map(key)    
    retValue
  }

  /**
   * 字符串解析
   * 找出需脱敏字段
   */
  def colRey(content: String, index: Int): Try[String] = Try {
    try {
      if (index > content.split(pre_flag).length)
        println("index :" + index + " out of content :" + content)
      val resultCol = content.split(pre_flag)(index)
      resultCol
    } catch {
      case e: Exception =>
        print("Exception :" + e.getMessage)        
        "ERROR"
    }
  }

//  /**
//   * 单字符串脱敏
//   */
//  def strRen(content: String, index: Int): Try[String] = Try {
//    val relCol = this.colRey(content, index).get;
//    val relColMd5 = this.md5Covert(relCol).get;
//    var strRturn = ""
//    var tmpStr = ""
//    var num = 0
//    for (i <- 0 until content.split(pre_flag).length) {
//      tmpStr = content.split(pre_flag)(i)
//      num = i
//      if (i == index) {
//        if (i == content.split(pre_flag).length - 1)
//          strRturn += relColMd5
//        else
//          strRturn += relColMd5 + "|"
//      } else if (i == content.split(pre_flag).length - 1)
//        strRturn += tmpStr
//      else
//        strRturn += tmpStr + "|"
//    }
//    strRturn
//  }
  
  /**
   * 单字符串脱敏
   */
  def strRen(content: String, index: Int): Try[String] = Try {
    val relCol = this.colRey(content, index).get;
    val relColMd5 = this.md5Covert(relCol).get;
    var strRturn = ""
    var tmpStr = ""
    var num = 0      
    val content_value=content.split(pre_flag)       
    for (i <- 0 until content_value.length) {
      tmpStr = content_value(i)
      num = i
      if (i == index) {
        if (i == content_value.length - 1)
          strRturn += relColMd5
        else
          strRturn += relColMd5 + "|"
      } else if (i == content_value.length - 1)
        strRturn += tmpStr
      else
        strRturn += tmpStr + "|"
    }
    strRturn
  } 
  
   
  
  
  /**
   * 单字符串屏蔽
   */
  def strReplace(content: String, index: Int): Try[String] = Try {
    val relCol = this.colRey(content, index).get;
    var strRturn = ""
    var tmpStr = ""
    var num = 0
    for (i <- 0 until content.split(pre_flag).length) {
      tmpStr = content.split(pre_flag)(i)
      num = i
      if (i != index) {
        if (i == content.split(pre_flag).length - 1)
          strRturn += tmpStr
        else if ((i == content.split(pre_flag).length - 2) && (index == content.split(pre_flag).length - 1))
          strRturn += tmpStr
        else
          strRturn += tmpStr + "|"
      }

    }
    strRturn
  }
  /**
   * 多符串脱敏
   */
  def strRens(content: String, desenFlags: String): Try[String] = Try {
   var strRturn=content
    for(desenFlag <- desenFlags.split(pre_flag)){
    strRturn = this.strRen(strRturn, desenFlag.toInt).get
    //println(desenFlag)
    }
   strRturn
  }
  /**
   * 多符串屏蔽
   */
  def strReplaces(content: String, replaceFlags: String): Try[String] = Try {
   var strRturn=content
   var replaceFlag =""   
   for (i <- 0 until replaceFlags.split(pre_flag).length) {     
     replaceFlag = replaceFlags.split(pre_flag)(i)    
    strRturn = this.strReplace(strRturn, (replaceFlag.toInt - i) ).get      
    }
   strRturn
  }
  //位数从0开始
  def processData (text: String): Try[String] = Try {
    val result = text.split("\\|")
    var value:String=""
    if(result.length==44){

      value+=result(0)+"|"+result(7)+"|"+result(17)+"|"+result(1)+"|"+result(2)+"|"+result(32)+"|"+result(33)+
        "|"+result(36)+"|"+result(22)+"|"+result(43)+  "\n"
    }
    value
  }
  
  //var value = this.ioInput("muliang").get
  //println(value)
  
  var content = {"18612632912|29845|31|6|1|1120|460012631619128|354442064948694|3GNET.MNC001.MCC460.GPRS|10.101.191.50|10.100.35.249|5|13|0|1|304|0|2015-12-09 16:45:21.208811|2015-12-09 16:46:26.377570|65|0|10.25.133.1|123.126.125.174|54347|443|0|0|0|0|0|0|0|0|220|0|4|0|220||||0|0|"}
//  //var content = { "1|555|111|2222|1122121|nihao" }
//  //var desenFlags = "0|2"
//   var replaceFlags = "1"
//   var desenFlag="0"
//  
    val zz = this.strRen(content, 0).get
//  val zz = this.strRens(content, "0|1|2|6").get
//    
//    //var str ="2015101509540700756011117.txt"
//    //val kk = str.substring(0,8)
//    //val kk = this.strReplaces(zz, replaceFlags).get +"\n"
// 
 println(zz)
//  

}
