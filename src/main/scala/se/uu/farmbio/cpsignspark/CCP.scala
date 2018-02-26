package se.uu.farmbio.cpsignspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import se.uu.it.easymr.EasyMapReduce
import java.io.File
import scopt.OptionParser
import se.uu.farmbio.cpsignspark.fcns._

case class CP(hs: Int, he: Int, fold: Int, cost: Int, seed: Int, file: String, CDK: String, label: String, p0: Double, p1: Double)

object CCP {

  case class Arglist(
    sparkMaster:    String   = "local",
    inputFolder:    String   = "",
    outputFolder:   String   = "",
    seedInput:      Seq[Int] = Seq(250),
    splitRatio:     Double   = 0.8f,
    swiftOpenstack: String   = "none",
    numberOfSplits: Int      = 1,
    heightStart:    Seq[Int] = Seq(1),
    heightEnd:      Seq[Int] = Seq(3),
    folds:          Seq[Int] = Seq(5),
    costs:          Seq[Int] = Seq(50))

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("CCP") {
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
      opt[Double]("splitRatio")
        .validate(x =>
          if (x > 0 && x <= 1) success
          else failure("Option must be >0"))
        .text("ProportionOfDataForTraining")
        .action((x, c) => c.copy(splitRatio = x))
      opt[String]("swiftOpenstack")
        .text("SwiftAddressForObjectStorage")
        .action((x, c) => c.copy(swiftOpenstack = x))
      opt[Int]("numberOfSplits")
        .validate(x =>
          if (x > 0) success
          else failure("Option must be >0"))
        .text("NumberOfSplitsForRDDofSDF")
        .action((x, c) => c.copy(numberOfSplits = x))
      opt[Seq[Int]]("heightStart")
        .validate(f =>
          if (f.min >= 0) success
          else failure("Need positive heights"))
        .text("HeightStartForSignature")
        .action((x, c) => c.copy(heightStart = x))
      opt[Seq[Int]]("heightEnd")
        .validate(f =>
          if (f.min >= 0) success
          else failure("Need positive heights"))
        .text("HeightEndForSignature")
        .action((x, c) => c.copy(heightEnd = x))
      opt[Seq[Int]]("folds")
        .validate(x =>
          if (x.min > 0) success
          else failure("Option must be >0"))
        .required()
        .text("FoldsForCCP")
        .action((x, c) => c.copy(folds = x))
      opt[Seq[Int]]("costs")
        .validate(x =>
          if (x.min >= 0) success
          else failure("Option must be >0"))
        .text("FoldsForCCP")
        .action((x, c) => c.copy(costs = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
    System.exit(0)
  }

  def run(params: Arglist) {

    //ARGS
    val inputFolder = params.inputFolder
    val outputFolder = params.outputFolder
    val splitRatio = params.splitRatio.toFloat
    val seedInput = params.seedInput.toList
    val sparkMaster = params.sparkMaster
    val swiftOpenstack = params.swiftOpenstack
    val numberOfSplits = params.numberOfSplits
    val heightStart = params.heightStart.toList
    val heightEnd = params.heightEnd.toList
    val folds = params.folds.toList
    val costs = params.costs.toList

    //SPARK CONF
    val conf = new SparkConf().setAppName("CCPpredictionCalibration")
    conf.setMaster(sparkMaster)
    val sc = new SparkContext(conf)
    if (swiftOpenstack != "none") sc.addJar(swiftOpenstack)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder().appName("CCPpredictionOutput").config("", "").getOrCreate()
    import spark.implicits._

    //PARAMETERS
    val parCpsign = ((((heightStart zip heightEnd cross folds)
      .map(z => (z._1._1, z._1._2, z._2))) cross costs)
      .map(z => (z._1._1, z._1._2, z._1._3, z._2))) // cross seedInput)
    //.map(z => (z._2, (z._1._1, z._1._2, z._1._3, z._1._4)))
    //.map(z => z.productIterator.toList.mkString("\n"))

    //READ DATA
    val rddSDFs = sc.wholeTextFiles(inputFolder, numberOfSplits).cache()
    //val wholeSDFs = new filesSDF(sc.wholeTextFiles(inputFolder, numberOfSplits))

    seedInput.map {
      case s => {
        val dataSet0 = toListSDFs(rddSDFs, splitRatio, s).cache()
        parCpsign.map {
          case (hs, he, f, c) => {
            println(f"heightStar: $hs, heightEnd: $he, fold:$f, cost:$c, seed:$s")
            val predictions = new EasyMapReduce(dataSet0
                .map(z => hs.toString  + "\n" + he.toString  + "\n" + f.toString + "\n"  + c.toString + "\n" + z))
              .setInputMountPoint("/dataSet.txt")
              .setOutputMountPoint("/out.txt")
              // Train
              .map(
                imageName = "indajuan/cpsign",
                command = // SPLIT STRING INTO DATASET, TRAIN, TEST
                  "head -n 6 /dataSet.txt  > pars && " + // save parameters
                    "export heightStart=$(head -1 pars | tr -d \"\n\") && " +
                    "export heightEnd=$(tail -n+2 pars | head -n 1 | tr -d \"\n\") && " +
                    "export folds=$(tail -n+3 pars | head -n 1 | tr -d \"\n\") && " +
                    "export costs=$(tail -n+4 pars | head -n 1 | tr -d \"\n\") && " +
                    "export seed=$(tail -n+5 pars | head -n 1 | tr -d \"\n\") && " +
                    "export dataName=$(tail -n+6 pars | head -n 1 | tr -d \"\n\") && " +
                    "tail -n+7 /dataSet.txt  > /dataSDF.txt && " + // remove the dataset name
                    "csplit -f set /dataSDF.txt /TESTSDFFILE/ && " + // split into train and test
                    "mv set00 /train.sdf && " + // rename and relocate
                    "tail -n+2 set01  > /test.sdf && " + // remove the TESTFFILE line
                    // TRAIN MODEL
                    "java -jar cpsign-0.6.6.jar train " + //start cpsign
                    "-t /train.sdf " + // train file
                    "-mn out " + //model name
                    "-mo /$dataName.cpsign " + //model out
                    "-c 3 " + // 3 CCP, 1 ACP
                    "-hs $heightStart " + // height start
                    "-he $heightEnd " + // height end
                    "-l [\"-1\",\"1\"] " + //labels
                    "-rn class " + //response name
                    "-nr $folds " + // number of folds
                    "--cost $costs " + // cost
                    "-i liblinear " + // liblinear or libsvm
                    "--license cpsign05-staffan-standard.license && " + //license
                    // PREDICT
                    "java -jar cpsign-0.6.6.jar predict " +
                    "--license cpsign05-staffan-standard.license " +
                    "-c 3 " +
                    "-m /$dataName.cpsign " +
                    "-p /test.sdf " +
                    "-o /out.txt ")
              .getRDD.map { json =>
                val parsedJson = parse(json)
                val key = compact(render(parsedJson \ "molecule" \ "cdk:Title"))
                val p0 = compact(render(parsedJson \ "prediction" \ "pValues" \ "-1")).toDouble
                val p1 = compact(render(parsedJson \ "prediction" \ "pValues" \ "1")).toDouble
                val label = compact(render(parsedJson \ "molecule" \ "class"))
                val fileN = compact(render(parsedJson \ "molecule" \ "file"))
                CP(hs, he, f, c, s, fileN, key, label, p0, p1)
              }
              .toDF.write.format("json").mode("overwrite")
              .save(outputFolder + "_" + hs.toString + "-" + he.toString + "-" + f.toString + "-" + c.toString + "-" + s.toString)
          }
        }
      }
    }

  }
}
    //-sparkMaster local -inputFolder src/test/resources/input -outputFolder src/test/resources/output -seedInput 250\
    //-splitRatio 0.8 -swiftOpenstack none -numberOfSplits 1 -heightStart 1 -heightEnd 3 -folds 3 -costs 50 -seedInput 100,250