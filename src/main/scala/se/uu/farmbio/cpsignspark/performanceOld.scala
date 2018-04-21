package se.uu.farmbio.cpsignspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

import java.io.File
import scopt.OptionParser

import se.uu.farmbio.cpsignspark.fcns._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset

case class Predict2(pars: String, qhts: String, molID: String,
                    label: String, splitSeed: Int, hs: Int, he: Int, fold: Int, cost: Double,
                    p0: Double, p1: Double, eval: Double, region: List[String],
                    regionSize: Int, correct: Int, unconfidence: Double, fuzziness: Double, excess: Int, mvalue: Int, sumP: Double,
                    singleCorrect: Int, incorrect: Int)

case class Validity2(pars: String, qhts: String, splitSeed: Int, hs: Int, he: Int, fold: Int, cost: Double, eval: Double, validity: Double,
                     numberCriterion: Double, mCriterion: Double, excessCriterion: Double, singleCorrect: Double, incorrect: Double)

case class ValidityPerLabel(pars: String, qhts: String, label:String, splitSeed: Int, hs: Int, he: Int, fold: Int, cost: Double, eval: Double, validity: Double,
                     numberCriterion: Double, mCriterion: Double, excessCriterion: Double, singleCorrect: Double, incorrect: Double)

case class MisCalibration2(pars: String, qhts: String, splitSeed: Int, hs: Int, he: Int, fold: Int, cost: Double, eval: Double,
                            validity: Double, errorRate: Double)
                            
case class MisCalibrationPerLabel(pars: String, qhts: String, label:String, splitSeed: Int, hs: Int, he: Int, fold: Int, cost: Double, eval: Double,
                            validity: Double, errorRate: Double)
                            

object performanceOld {

  case class Arglist(
    sparkMaster:    String = "local",
    inputFolder:    String = "",
    outputFolder:   String = "",
    swiftOpenstack: String = "none",
    minEpsilon:     Double = 0,
    nEpsilon:       Int    = 10,
    maxEpsilon:     Double = 1)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("CCP") {
      head("Cross Conformal Prediction performance evaluation old.")
      opt[String]("sparkMaster")
        .text("SparkMaster")
        .action((x, c) => c.copy(sparkMaster = x))
      opt[String]("inputFolder")
        .required()
        .text("predictionFiles")
        .action((x, c) => c.copy(inputFolder = x))
      opt[String]("outputFolder")
        .required()
        .text("PathToOutputPredictionfiles")
        .action((x, c) => c.copy(outputFolder = x))
      opt[String]("swiftOpenstack")
        .text("SwiftAddressForObjectStorage")
        .action((x, c) => c.copy(swiftOpenstack = x))
      opt[Int]("nEpsilon")
        .validate(x =>
          if (x > 0) success
          else failure("Integer with the number of epsilons to evaluate"))
        .text("epsilon")
        .action((x, c) => c.copy(nEpsilon = x))
      opt[Double]("minEpsilon")
        .validate(x =>
          if (x >= 0 & x < 1) success
          else failure("minimum epsilon to evaluate"))
        .text("minEpsilon")
        .action((x, c) => c.copy(minEpsilon = x))
      opt[Double]("maxEpsilon")
        .validate(x =>
          if (x > 0 & x <= 1) success
          else failure("minimum epsilon to evaluate"))
        .text("epsilon")
        .action((x, c) => c.copy(maxEpsilon = x))

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
    val inputFile = params.inputFolder
    val outputFolder = params.outputFolder
    val sparkMaster = params.sparkMaster
    val swiftOpenstack = params.swiftOpenstack
    val nEpsilon = params.nEpsilon
    val minEpsilon = params.minEpsilon
    val maxEpsilon = params.maxEpsilon
    val r = 10000000

    //SPARK CONF
    val conf = new SparkConf().setAppName("performanceOld")
    if (sparkMaster == "local") conf.setMaster("local")
    val sc = new SparkContext(conf)
    if (swiftOpenstack != "none") sc.addJar(swiftOpenstack)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder().appName("performanceOld").config("", "").getOrCreate()

    if (minEpsilon >= maxEpsilon) println("minEpsilon is equal or larger than maxEpsilon.\n")
    val epsilon = (minEpsilon * r to maxEpsilon * r by (math.round((maxEpsilon * r - minEpsilon * r) / (nEpsilon)))).map(e => e.toDouble / r)
    println("\nEpsilon range between " + minEpsilon + " and: " + epsilon.last + "\n")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val customSchema = StructType(Array(
      StructField("qhts", StringType, true),
      StructField("molID", StringType, true),
      StructField("label", StringType, true),
      StructField("splitSeed", IntegerType, true),
      StructField("heightStart", IntegerType, true),
      StructField("heightEnd", IntegerType, true),
      StructField("folds", IntegerType, true),
      StructField("cost", DoubleType, true),
      StructField("p0", DoubleType, true),
      StructField("p1", DoubleType, true)))

    val df = spark.read.schema(customSchema).option("header", true).option("delimiter", ",").csv(inputFile + "*.csv")
    val ds = df.as[CP].persist()

    ds.printSchema()

    val efficiency = ds.map(z => makePrediction(z, -1)).persist()

    // efficiency all
    
    efficiency.groupBy("pars", "qhts", "splitSeed", "hs", "he", "fold", "cost")
      .agg(
        avg("sumP") as "sumCriterion",
        avg("unconfidence") as "unconfidenceCriterion",
        avg("fuzziness") as "fuzzinessCriterion")
      .sort($"pars")
      .repartition(1).write.format("csv").option("header", "true").mode("overwrite").save(outputFolder + "efficiency")

    println("\nSaved efficiency for: " + inputFile.split("/").last)

    // efficiency per label 
    efficiency.groupBy("pars", "qhts", "label", "splitSeed", "hs", "he", "fold", "cost")
      .agg(
        avg("sumP") as "sumCriterion",
        avg("unconfidence") as "unconfidenceCriterion",
        avg("fuzziness") as "fuzzinessCriterion")
      .sort($"pars")
      .repartition(1).write.format("csv").option("header", "true").mode("overwrite").save(outputFolder + "efficiencyPerLabel")

    println("\nSaved efficiency per label for: " + inputFile.split("/").last)


    val validity = ds.flatMap(z => {
      epsilon.map { e => makePrediction(z, e) }
    }).persist()

    //validity all
    val validityAll = validity.groupBy("pars", "qhts", "splitSeed", "hs", "he", "fold", "cost", "eval")
      .agg(
        avg("correct") as "validity",
        avg("regionSize") as "numberCriterion",
        avg("mvalue") as "mCriterion",
        avg("excess") as "excessCriterion",
        avg("singleCorrect") as "singleCorrect",
        avg("incorrect") as "incorrect")
      .sort($"pars", $"eval").as[Validity2]

      
    validityAll.repartition(1).write.format("csv").option("header", "true").mode("overwrite").save(outputFolder + "validity")
    println("\nSaved validity for: " + inputFile.split("/").last)
    
    val validityPerLabel = validity.groupBy("pars", "qhts","label", "splitSeed", "hs", "he", "fold", "cost", "eval")
      .agg(
        avg("correct") as "validity",
        avg("regionSize") as "numberCriterion",
        avg("mvalue") as "mCriterion",
        avg("excess") as "excessCriterion",
        avg("singleCorrect") as "singleCorrect",
        avg("incorrect") as "incorrect")
      .sort($"pars", $"eval").as[ValidityPerLabel]

    validityPerLabel.repartition(1).write.format("csv").option("header", "true").mode("overwrite").save(outputFolder + "validityPerLabel")
    println("\nSaved validity per label for: " + inputFile.split("/").last)
    
    // miscalibration all
   
    validityAll.map {
      case Validity2(pars, qhts, splitSeed, hs, he, fold, cost, eval, validity, numberCriterion, mCriterion, excessCriterion, singleCorrect,incorrect) =>
        new MisCalibration2(pars, qhts, splitSeed, hs, he, fold, cost, eval, validity, Math.abs((1 - validity) - eval))
    }
    .groupBy("pars", "qhts", "splitSeed", "hs", "he", "fold", "cost").agg(sum("errorRate") as "errorRate")
    .repartition(1).write.format("csv").option("header", "true").mode("overwrite").save(outputFolder+"miscalibration")
    println("\nSaved miscalibration for: " + inputFile.split("/").last)

    // miscalibration per label
    validityPerLabel.map {
      case ValidityPerLabel(pars, qhts, label, splitSeed, hs, he, fold, cost, eval, validity, numberCriterion, mCriterion, excessCriterion, singleCorrect,incorrect) =>
        new MisCalibrationPerLabel(pars, qhts, label, splitSeed, hs, he, fold, cost, eval, validity, Math.abs((1 - validity) - eval))
    }
    .groupBy("pars", "qhts", "label", "splitSeed", "hs", "he", "fold", "cost").agg(sum("errorRate") as "errorRate")
    .repartition(1).write.format("csv").option("header", "true").mode("overwrite").save(outputFolder+"miscalibrationPerLabel")
    println("\nSaved miscalibration per label for: " + inputFile.split("/").last)

    
    /*
  */

    sc.stop()
  }

  def makePrediction(cp: CP, epsilon: Double): Predict2 = {
    cp match {
      case CP(qhts, molID, label, splitSeed, hs, he, fold, cost, p0, p1) =>

        val pars = qhts + "_" + splitSeed + "_" + hs + "_" + he +
          "_" + fold + "_" + cost

        var region = Set.empty[String]
        if (p0 > epsilon) region += "-1"
        if (p1 > epsilon) region += "1"

        val regionSize = region.size
        val correct = if (region(label)) 1 else 0

        val unconfidence = if (label == "1") p0 else p1
        val fuzziness = if (label == "1") p0 else p1
        val excess = if (region(label)) regionSize - 1 else regionSize
        val mvalue = if (excess != 0) 1 else 0
        val sumP = p0 + p1
        val singleCorrect = if (correct == 1 && regionSize == 1) 1 else 0
        val incorrect = 1 - correct

        new Predict2(pars, qhts, molID, label, splitSeed, hs, he, fold, cost, p0, p1, epsilon, region.toList,
          regionSize, correct, unconfidence, fuzziness, excess, mvalue, sumP, singleCorrect, incorrect)
    }
  }

}
