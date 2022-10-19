package commons

import org.apache.log4j.{LogManager, Logger}

import java.io.{PrintWriter, StringWriter}

/***
 *
 * @param classname   get classname
 * @param myMsg       user defined error message
 * @param error       actual exception
 */
case class MyException(classname : Class[_], myMsg : String, error : Throwable) extends Exception {

  //REFER
  //https://alvinalexander.com/scala/how-convert-stack-trace-exception-string-print-logger-logging-log4j-slf4j/
  val sw = new StringWriter
  error.printStackTrace(new PrintWriter(sw))
  sw.toString

  val log : Logger = LogManager.getLogger(classname.getName)

  //TODO : send email notification

  //REFER
  //https://www.geeksforgeeks.org/throw-keyword-in-scala/#:~:text=The%20throw%20keyword%20in%20Scala,and%20scala%20are%20very%20similar.
  throw new Exception("error : " + " " + classname.getSimpleName + " " + myMsg + " " + error +  " " + error.getStackTrace)
}
