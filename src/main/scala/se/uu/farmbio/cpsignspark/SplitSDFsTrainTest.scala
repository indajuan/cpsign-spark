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

case class DSTrTe(fileName: String, Train: Boolean, sdfs: List[IAtomContainer])
case class DS1(fileName: String, Train: Boolean, SDFs: String)

object SplitSDFsTrainTest {
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
        

    // Convert a List[IAtomContainer] object to string in SDF format
    def toSDF(mols: IAtomContainer): String = {
      val strWriter = new StringWriter()
      val writer = new SDFWriter(strWriter)
      mols.removeProperty("cdk:Remark")
      writer.write(mols)
      writer.close
      strWriter.toString()
    }

    // Create a RDD[DS]
    println(wholeSDFs.partitions.size)
    
    val cdks = wholeSDFs.flatMap { 
      case (fileName, sdfs) => 
        val sdfByteArray = sdfs.getBytes(Charset.forName("UTF-8"))
        val sdfIS = new ByteArrayInputStream(sdfByteArray) //Parse SDF
        val reader = new MDLV2000Reader(sdfIS)
        val chemFile = reader.read(new ChemFile)
        val mols1 = ChemFileManipulator.getAllAtomContainers(chemFile)
        reader.close
        Random.setSeed(seedInput)
        val mols = Random.shuffle(mols1.toList)
        val posMols = mols.filter(_.getProperty("class") == "1").toList 
        val negMols = mols.filter(_.getProperty("class") == "-1").toList

        val (posTrain, posTest) = posMols.toList
          .splitAt(Math.round(posMols.length * splitRatio))

        val (negTrain, negTest) = negMols.toList
          .splitAt(Math.round(negMols.length * splitRatio))

        Seq(
          DSTrTe(fileName.split(inputFolder + "/").last, true , posTrain ++ negTrain),
          DSTrTe(fileName.split(inputFolder + "/").last, false, posTest ++ negTest))
    }
    cdks.flatMap(t => t.sdfs.map(z => DS1(t.fileName, t.Train, toSDF(z))))
      .toDF.write.format("json").mode("overwrite").save(outputFolder)
    sc.stop()
  }
}

// If you want to print each SDF file to different output files 
//.toDF

//    def convertRowToJSON(row: Row): String = {
//      val m = row.getValuesMap(row.schema.fieldNames)
//      JSONObject(m).toString()
//    }
    
//    wholeSDFs.select($"fileName",$"Train").foreach{
//     z => {
//       val n = z(0)
//       val x = convertRowToJSON(z)
//       val file = new File(outputFolder+"/"+n+".train")
//       val bw = new BufferedWriter(new FileWriter(file))
//        bw.write(x)
//        bw.close()
//     }  }
//    wholeSDFs.select($"fileName",$"Test").foreach{
//     z => {
//       val n = z(0)
//       val x = convertRowToJSON(z)
//       val file = new File(outputFolder+"/"+n+".test")
//       val bw = new BufferedWriter(new FileWriter(file))
//        bw.write(x)
//        bw.close()
//     }  }
    
// To run:
// <input dir src/test/resources/input> <output dir <src/test/resources/output>\
// <splitratio 0.8> <seed 250> <master local> <jar added none> <numberOfSplits 1>
// need to shuffle before submitting to cpsign or ML algorithm 

