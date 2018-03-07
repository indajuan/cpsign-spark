package se.uu.farmbio.cpsignspark

import java.io._
import java.io.ByteArrayInputStream
import java.io.StringWriter
import java.nio.charset.Charset
import scala.collection.JavaConversions.asJavaIterator
import scala.collection.JavaConversions.asScalaBuffer
import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object fcns {

  implicit class Crossable[X](xs: List[X]) {
    def cross[Y](ys: List[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def toTrainTestList(rdd: RDD[(String, String)]): RDD[(String, List[String], List[String])] =
    rdd.map {
      case (f, sdfs) => {
        val fn = f.split("/").last.split("\\.").head
        (
          fn,
          sdfs.replaceAll("\\$\\$\\$\\$\n\n", "\\$\\$\\$\\$\n")
          .replaceAll("\\$\\$\\$\\$\n", "\\$\\$\\$\\$")
          .split("\\$\\$\\$\\$").filter(z => z.contains("class"))
          .filter(_.contains(">  <class>\n1")).toList,
          sdfs.split("\\$\\$\\$\\$").filter(z => z.contains("class"))
          .filter(_.contains(">  <class>\n-1")).toList)
      }
    }

  def expandSDF(sdfFiles: RDD[(String, List[String], List[String])], parCpsign: List[(Int, Int, Int, Int)],
                seedInput: List[Int], splitRatio: Float): RDD[String] = {
    sdfFiles.flatMap {
      case (fname, l1, l0) =>
        seedInput.flatMap {
          case (s) =>
            parCpsign.map {
              case (hs, he, f, c) =>
                val nl1 = l1.length
                val nl0 = l0.length
                Random.setSeed(s)
                val (trainl1, testl1) = Random.shuffle(l1).splitAt(Math.ceil(splitRatio * nl1).toInt)
                Random.setSeed(s)
                val (trainl0, testl0) = Random.shuffle(l0).splitAt(Math.ceil(splitRatio * nl0).toInt)
                val train = (trainl1 ++ trainl0).map(z => z + "\n>  <file>\n" + fname + "\n" +
                  "\n> <hs>\n" + hs + "\n" + "\n>  <he>\n" + he + "\n" + "\n>  <f>\n" + f + "\n" + "\n>  <c>\n" + c + "\n>  <s>\n" + s)
                val test = (testl1 ++ testl0).map(z => z + "\n>  <file>\n" + fname + "\n" +
                  "\n> <hs>\n" + hs + "\n" + "\n>  <he>\n" + he + "\n" + "\n>  <f>\n" + f + "\n" + "\n>  <c>\n" + c + "\n>  <s>\n" + s)
                hs + "\n" + he + "\n" + f + "\n" + c + "\n" + s + "\n" + (test.mkString("\n$$$$\n") + "\n$$$$").replaceAll("\\$\\$\\$\\$\n\n", "\\$\\$\\$\\$\n") +
                  "\nTRAININGSET\n" +
                  (train.mkString("\n$$$$\n") + "\n$$$$").replaceAll("\\$\\$\\$\\$\n\n", "\\$\\$\\$\\$\n")
            }
        }
    }

  }

}



