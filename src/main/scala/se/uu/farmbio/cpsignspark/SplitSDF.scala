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
import org.openscience.cdk.ChemFile
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.io.MDLV2000Reader
import org.openscience.cdk.io.SDFWriter
import org.openscience.cdk.tools.manipulator.ChemFileManipulator

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

case class DS(fileName: String, isTrain: Boolean, data: String)

object SplitSDF {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Splitter")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("SplitSDF")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    //val sqlContext = new SQLContext(sc)

    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext._
    //import sqlContext.implicits._

    val inputFolder = args(0)
    val outputFolder = args(1)
    val splitRatio = args(2).toFloat

    // Convert a List[IAtomContainer] object to string in SDF format
    def toSDF(mols: List[IAtomContainer]): String = {
      val strWriter = new StringWriter()
      val writer = new SDFWriter(strWriter)
      val molsIt = mols.iterator
      while (molsIt.hasNext()) {
        val mol = molsIt.next
        writer.write(mol)
      }
      writer.close
      strWriter.toString()
    }

    // Define a case class DS

    // Create a RDD[DS]
    val wholeSDFs = sc.wholeTextFiles(inputFolder) //read all the files in directory "data/"
      .flatMap { //"flat" the two objects train and test into one RDD
        case (fileName, sdfs) => //cas(fileName, sdfs)   conserves the filename read at wholeTextFile
          val sdfByteArray = sdfs
            .getBytes(Charset.forName("UTF-8"))
          val sdfIS = new ByteArrayInputStream(sdfByteArray) //Parse SDF
          val reader = new MDLV2000Reader(sdfIS)
          val chemFile = reader.read(new ChemFile)
          val mols = ChemFileManipulator.getAllAtomContainers(chemFile)
          reader.close

          val posMols = mols.filter(_.getProperty("class") == "1").toList // filter class = 1
          val negMols = mols.filter(_.getProperty("class") == "-1").toList // filter class = -1

          val (posTrain, posTest) = Random.shuffle(posMols.toList)
            .splitAt(Math.round(posMols.length * splitRatio)) // shuffle the positive examples and split them
          val (negTrain, negTest) = Random.shuffle(negMols.toList)
            .splitAt(Math.round(negMols.length * splitRatio)) // shuffle the negative examples and split them

          val trainSet = Random.shuffle(posTrain ++ negTrain) // put together pos and neg training and shuffle
          val testSet = Random.shuffle(posTest ++ negTest) // put together pos and neg test and shuffle
          Seq(
            DS(fileName, true, toSDF(trainSet)),
            DS(fileName, false, toSDF(testSet)))
      }

    wholeSDFs.toDF.write.format("json").mode("overwrite").save(outputFolder)
    sc.stop()

  }
}