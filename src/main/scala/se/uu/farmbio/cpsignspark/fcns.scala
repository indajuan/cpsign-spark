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

object fcns {
  
  implicit class Crossable[X](xs: List[X]) {
      def cross[Y](ys: List[Y]) = for { x <- xs; y <- ys } yield (x, y)
    }
  
  def toListSDFs(rdd: RDD[(String, String)], splitRatio: Float, seedInput: Int): RDD[String] = {
    rdd.flatMap {
      case (fileName, sdfs) =>
        Random.setSeed(seedInput)
        val fn = fileName.split("/").last
        // Split string by $$$$, remove molecules with no class (last entry)
        val sdfsList = Random.shuffle(sdfs.split("\\$\\$\\$\\$")
          .filter(z => z.contains("class"))
          .map(z => (z, z.contains(">  <class>\\n1")))
          .toList)

        // Split in train and test for class 1 and class -1
        val (posTrain, posTest) = sdfsList.filter(_._2)
          .splitAt(Math.round(sdfsList.length * splitRatio))
        val (negTrain, negTest) = sdfsList.filter(!_._2)
          .splitAt(Math.round(sdfsList.length * splitRatio))

        Random.setSeed(seedInput)
        Seq( seedInput.toString + "\n" + fn + "\n"  +
              Random.shuffle(posTrain ++ negTrain).map(z => z._1)
              .mkString(">  <file>\n" + fn +  "\n$$$$") + "\n>  <file>\n" + fn + "\n$$$$" + "\nTESTSDFFILE\n" +
              Random.shuffle(posTest ++ negTest).map(z => z._1)
              .mkString(">  <file>\n" + fn +  "\n$$$$") + "\n>  <file>\n" + fn + "\n$$$$")
    }
  }
   
}



