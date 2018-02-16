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

case class DataSetList(fileName: String, Train: Boolean, SDFs: List[String])
case class DataSet(fileName: String, Train: Boolean, SDFs: String)

object SplitTrainTest {
  def main(args: Array[String]) {
    val inputFolder = args(0)
    val outputFolder = args(1)
    val splitRatio = args(2).toFloat
    val seedInput = args(3).toInt
    val sparkmaster = args(4)
    val swiftOpenstack = args(5)
    val numberOfSplits = args(6).toInt

    val conf = new SparkConf().setAppName("SplitterTestTrain")
    if (sparkmaster == "local") conf.setMaster("local")

    val sc = new SparkContext(conf)
    if (swiftOpenstack != "none") sc.addJar(args(5))

    sc.setLogLevel("WARN")

    val spark = SparkSession.builder().appName("SplitTrainTest").config("", "").getOrCreate()

    import spark.implicits._

    //read all the files in directory
    val wholeSDFs = sc.wholeTextFiles(inputFolder, numberOfSplits) 
    
    
    // print splits of rdd
    println("numberOfSplits:  " + wholeSDFs.partitions.size)
    
    
    // flatmap the files' SDFs into one RDD with train, test data
    val tt = wholeSDFs.flatMap { 
      case (fileName, sdfs) =>
        Random.setSeed(seedInput)
        
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
        
        //Return values, concatenate class -1 and 1 training and testing into one list each, return value
        Seq(
          DataSetList(fileName.split(inputFolder + "/").last, true, (posTrain ++ negTrain).map(z => z._1)),
          DataSetList(fileName.split(inputFolder + "/").last, false, (posTest ++ negTest).map(z => z._1)))
    }
    
    // flat the training and testing list to one row per element in the lists, to dataframe, write to json
    tt.flatMap(t => t.SDFs.map(z => DataSet(t.fileName, t.Train, z)))
      .toDF.write.format("json").mode("overwrite").save(outputFolder)
    sc.stop()
  }
}

// To run:
// <input dir src/test/resources/input> <output dir <src/test/resources/output>\
// <splitratio 0.8> <seed 250> <master local> <jar added none> <numberOfSplits 1>
// need to shuffle before submitting to cpsign or ML algorithm 

