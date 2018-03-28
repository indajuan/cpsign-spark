package se.uu.farmbio.cpsignspark

import java.io.BufferedWriter
import java.io.FileWriter
import java.io.BufferedOutputStream
import java.io.FileOutputStream

import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scopt.OptionParser
import se.uu.farmbio.cpsignspark.fcns._
import org.apache.hadoop.conf.Configuration
import java.io.OutputStreamWriter

object splitterSDF {
  case class Arglist(
    sparkMaster:    String   = "local",
    inputFolder:    String   = "",
    outputFolder:   String   = "",
    seedInput:      Seq[Int] = Seq(250),
    splitRatio:     Double   = 0.8f,
    numberOfSplits: Int      = 1,
    swiftOpenstack: String   = "none")

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("splitterSDF") {
      head("Cross Conformal Prediction calibration.")
      opt[String]("sparkMaster")
        .text("SparkMaster")
        .action((x, c) => c.copy(sparkMaster = x))
      opt[String]("inputFolder")
        .required()
        .text("PathToInputSDFfiles")
        .action((x, c) => c.copy(inputFolder = x))
      opt[String]("outputFolder")
        .required()
        .text("PathToOutputPredictionfiles")
        .action((x, c) => c.copy(outputFolder = x))
      opt[Seq[Int]]("seedInput")
        .text("IntSeedsForRandomSplitting")
        .action((x, c) => c.copy(seedInput = x))
      opt[Int]("numberOfSplits")
        .validate(x =>
          if (x > 0) success
          else failure("Option must be >0"))
        .text("NumberOfSplitsForRDDofSDF")
        .action((x, c) => c.copy(numberOfSplits = x))
      opt[Double]("splitRatio")
        .validate(x =>
          if (x > 0 && x <= 1) success
          else failure("Option must be >0"))
        .text("ProportionOfDataForTraining")
        .action((x, c) => c.copy(splitRatio = x))
      opt[String]("swiftOpenstack")
        .text("SwiftAddressForObjectStorage")
        .action((x, c) => c.copy(swiftOpenstack = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
    //System.exit(0)
  }

  def run(params: Arglist) {

    //ARGS
    val inputFolder = params.inputFolder
    val outputFolder = params.outputFolder
    val splitRatio = params.splitRatio.toFloat
    val seedInput = params.seedInput.toList
    val numberOfSplits = params.numberOfSplits
    val sparkMaster = params.sparkMaster
    val swiftOpenstack = params.swiftOpenstack

    //SPARK CONF
    val conf = new SparkConf().setAppName("splitterSDF")
    if (sparkMaster == "local") conf.setMaster("local")
    val sc = new SparkContext(conf)
    if (swiftOpenstack != "none") sc.addJar(swiftOpenstack)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder().appName("splitterSDF").config("", "").getOrCreate()
    //PARAMETERS
    val sdfFiles = sc.wholeTextFiles(inputFolder, numberOfSplits).persist()
    val listFiles = sdfFiles.keys.collect().toList
    println("Files to split: \n" + listFiles)
    val sdfFilesList = splitByLabel(sdfFiles)
    val seedSDFList = splitNameTestTrain(sdfFilesList, seedInput, splitRatio, numberOfSplits)

    for (f <- seedSDFList) {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val test = f._2
      val train = f._3
      val file1 = f._1 + "_tst.sdf"
      val file2 = f._1 + "_trn.sdf"

      val path1: Path = new Path(outputFolder  + file1)
      if (fs.exists(path1)) {
        fs.delete(path1, true)
      }
      val dataOutputStream1: FSDataOutputStream = fs.create(path1)
      val bw1: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream1, "UTF-8"))
      for (s <- test) {
        bw1.write(s)
      }
      bw1.close
      
      val path2: Path = new Path(outputFolder  + file2)
      if (fs.exists(path2)) {
        fs.delete(path2, true)
      }
      val dataOutputStream2: FSDataOutputStream = fs.create(path2)
      val bw2: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream2, "UTF-8"))
      for (s <- train) {
        bw2.write(s)
      }
      bw2.close
    }
   sc.stop() 
  }
}
