package se.uu.farmbio.cpsignspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel


import scopt.OptionParser

object validityCCP {

  case class Arglist(
    sparkMaster:    String = "local",
    inputFolder:    String = "",
    outputFolder:   String = "",
    swiftOpenstack: String = "none",
    minEpsilon:     Double = 0,
    nEpsilon:       Int    = 10,
    numberOfSplits: Int      = 1,
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
        opt[Int]("numberOfSplits")
        .validate(x =>
          if (x > 0) success
          else failure("Option must be >0"))
        .text("NumberOfSplitsForRDDofSDF")
        .action((x, c) => c.copy(numberOfSplits = x))
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
    val r = 10000
    val numberOfSplits = params.numberOfSplits

    val conf = new SparkConf().setAppName("validity")
    if (sparkMaster == "local") conf.setMaster("local")
    val sc = new SparkContext(conf)
    if (swiftOpenstack != "none") sc.addJar(swiftOpenstack)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder().appName("performanceOld").config("", "").getOrCreate()

    if (minEpsilon >= maxEpsilon) println("minEpsilon is equal or larger than maxEpsilon.\n")
    val epsilon = (minEpsilon * r to maxEpsilon * r by (math.round((maxEpsilon * r - minEpsilon * r) / (nEpsilon)))).map(e => e.toDouble / r)
    println("\nEpsilon range between " + minEpsilon + " and: " + epsilon.last + " by: " + (epsilon(1) - epsilon(0)) + "\n")

    val predictionSchema = StructType(Array(
      StructField("qhts", StringType, true),
      StructField("label", StringType, true),
      StructField("splitSeed", IntegerType, true),
      StructField("heightStart", IntegerType, true),
      StructField("heightEnd", IntegerType, true),
      StructField("folds", IntegerType, true),
      StructField("cost", DoubleType, true),
      StructField("p0", DoubleType, true),
      StructField("p1", DoubleType, true),
      StructField("eval", DoubleType, true),
      StructField("region", StringType, true),
      StructField("regionSize", IntegerType, true),
      StructField("valid", IntegerType, true),
      StructField("fuzziness", DoubleType, true),
      StructField("excess", IntegerType, true),
      StructField("sumP", DoubleType, true),
      StructField("singleCorrect", IntegerType, true)))

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.scalalang.typed

    def read(resource: String): RDD[CP] = {
      println("Reading files\n")
      val rdd = spark.sparkContext.textFile(resource,numberOfSplits)
      val headerColumns = rdd.first()
      rdd.filter(row => row != headerColumns)
        .map(_.split(",").to[List])
        .map(makeCP)
    }

    def makeCP(line: List[String]): CP = {
      new CP(line(0), line(1), line(2),
        line(3).toInt, line(4).toInt, line(5).toInt, line(6).toInt,
        line(7).toDouble, line(8).toDouble, line(9).toDouble)
    }

    def makePredictionRegions(rdd: RDD[CP], epsilon: Double): RDD[PredictionRegion] = {

      rdd.map(r => {
        var region = Set.empty[String]
        if (r.p0 > epsilon) region += "-1"
        if (r.p1 > epsilon) region += "1"
        val regionSize = region.size
        val valid = if (region(r.label)) 1 else 0
        val fuzziness = if (r.label == "-1") r.p1 else r.p0
        val excess = if (region(r.label)) (regionSize - 1) else regionSize
        val singleCorrect = if (valid == 1 && regionSize == 1) 1 else 0
        new PredictionRegion(r.qhts, r.label, r.splitSeed, r.heightStart, r.heightEnd,
          r.folds, r.cost, r.p0, r.p1, epsilon, region.toList.mkString(","), regionSize, valid,
          fuzziness, excess, r.p0 + r.p1, singleCorrect)
      })
    }

    //val ds = spark.createDataset(rdd)

    def makePerformanceEDependent(ds: Dataset[PredictionRegion]):Dataset[PerformanceEDependent]= {

      val eDependent = ds.groupByKey(r => (r.qhts, r.splitSeed, r.heightStart,
        r.heightEnd, r.folds, r.cost, r.eval))
        .agg(
          round(typed.avg[PredictionRegion](_.regionSize), 5).as[Double],
          round(typed.avg[PredictionRegion](_.valid), 5).as[Double],
          round(typed.avg[PredictionRegion](_.excess), 5).as[Double],
          round(typed.avg[PredictionRegion](_.singleCorrect), 5).as[Double])
        .map(z => PerformanceEDependent(z._1._1, z._1._2, z._1._3, z._1._4,
          z._1._5, z._1._6, z._1._7, z._2, z._3, z._4, z._5))
        .orderBy("splitSeed", "heightStart", "heightEnd", "folds", "cost", "eval")
        
        eDependent
    }
    
    
    def makePerformanceEDependent2(ds: Dataset[PredictionRegion]): Dataset[PerformanceEDependentLabel] = {

    val eDependentLabel = ds.groupByKey(r => (r.qhts, r.label, r.splitSeed, r.heightStart,
        r.heightEnd, r.folds, r.cost, r.eval))
        .agg(
          round(typed.avg[PredictionRegion](_.regionSize), 5).as[Double],
          round(typed.avg[PredictionRegion](_.valid), 5).as[Double],
          round(typed.avg[PredictionRegion](_.excess), 5).as[Double],
          round(typed.avg[PredictionRegion](_.singleCorrect), 5).as[Double])
        .map(z => PerformanceEDependentLabel(z._1._1, z._1._2, z._1._3, z._1._4,
          z._1._5, z._1._6, z._1._7, z._1._8, z._2, z._3, z._4, z._5))
        .orderBy("splitSeed", "heightStart", "heightEnd", "folds", "cost", "eval")
        
        eDependentLabel
    }
    
    def makePerformanceEIndependent(ds: Dataset[PredictionRegion]): (Dataset[PerformanceEIndependent], Dataset[PerformanceEIndependentLabel]) = {

      val eIndependent = ds.groupByKey(r => (r.qhts, r.splitSeed, r.heightStart,
        r.heightEnd, r.folds, r.cost))
        .agg(
          round(typed.avg[PredictionRegion](_.fuzziness), 5).as[Double],
          round(typed.avg[PredictionRegion](_.sumP), 5).as[Double])
        .map(z => PerformanceEIndependent(z._1._1, z._1._2, z._1._3, z._1._4,
          z._1._5, z._1._6, z._2, z._3))
        .orderBy("splitSeed", "heightStart", "heightEnd", "folds", "cost")

      val eIndependentLabel = ds.groupByKey(r => (r.qhts, r.label, r.splitSeed, r.heightStart,
        r.heightEnd, r.folds, r.cost, r.label))
        .agg(
          round(typed.avg[PredictionRegion](_.fuzziness), 5).as[Double],
          round(typed.avg[PredictionRegion](_.sumP), 5).as[Double])
        .map(z => PerformanceEIndependentLabel(z._1._1, z._1._2, z._1._3, z._1._4,
          z._1._5, z._1._6, z._1._7, z._2, z._3))
        .orderBy("splitSeed", "heightStart", "heightEnd", "folds", "cost")

      (eIndependent, eIndependentLabel)

    }

    
    val predictions = read(inputFile ).repartition(numberOfSplits).persist(StorageLevel.DISK_ONLY)
    
    println("making perfromance e-independent")
    val predictionRegionEIndependent = makePredictionRegions(predictions, -1)
    val (performanceEIndependent, performanceEIndependentLabel) = makePerformanceEIndependent(
      spark.createDataset(predictionRegionEIndependent))
    performanceEIndependent.repartition(1).write.format("csv").option("header", "true").option("compression", "gzip").mode("overwrite").save(outputFolder + "efficiency")
    println("Saved: " + outputFolder + "efficiency\n")
    performanceEIndependentLabel.repartition(1).write.format("csv").option("header", "true").option("compression", "gzip").mode("overwrite").save(outputFolder + "efficiencyPerLabel")
    println("Saved: " + outputFolder + "efficiencyPerLabel\n")

    println("making perfromance e-dependent")
    
    
    val performanceEpsilonDependent = epsilon.map {
      case e =>
        val predictionRegionEDependent = makePredictionRegions(predictions, e).persist()
        val performanceEDependent = makePerformanceEDependent(
          spark.createDataset(predictionRegionEDependent))
        
        performanceEDependent.repartition(1).write.format("csv").option("header", "true").option("compression", "gzip").mode("overwrite").save(outputFolder + "performanceE" + e)
        println("Saved: " + outputFolder + "performanceE: " + e ) 
        
    
        val performanceEDependentLabel = makePerformanceEDependent2(
          spark.createDataset(predictionRegionEDependent))
          
        performanceEDependentLabel.repartition(1).write.format("csv").option("header", "true").mode("overwrite").option("compression", "gzip").save(outputFolder + "performanceEPerLabel" + e)
        
        println("Saved: " + outputFolder + "performanceEPerLabel: " + e)
        predictionRegionEDependent.unpersist()
    }
     
  }

  case class PredictionRegion(
    qhts: String, label: String, splitSeed: Int, heightStart: Int, heightEnd: Int,
    folds: Int, cost: Double, p0: Double, p1: Double, eval: Double,
    region: String, regionSize: Int, valid: Int,
    fuzziness: Double, excess: Int, sumP: Double, singleCorrect: Int)

  case class PerformanceEDependent(
    qhts: String, splitSeed: Int, heightStart: Int, heightEnd: Int,
    folds: Int, cost: Double, eval: Double, regionSize: Double, valid: Double,
    excess: Double, singleCorrect: Double)

  case class PerformanceEDependentLabel(
    qhts: String, label: String, splitSeed: Int, heightStart: Int, heightEnd: Int,
    folds: Int, cost: Double, eval: Double, regionSize: Double, valid: Double,
    excess: Double, singleCorrect: Double)

  case class PerformanceEIndependent(
    qhts: String, splitSeed: Int, heightStart: Int, heightEnd: Int,
    folds: Int, cost: Double, fuzziness: Double, sumP: Double)

  case class PerformanceEIndependentLabel(
    qhts: String, label: String, splitSeed: Int, heightStart: Int, heightEnd: Int,
    folds: Int, cost: Double, fuzziness: Double, sumP: Double)

  case class Miscalibration(
    qhts: String, splitSeed: Int, heightStart: Int, heightEnd: Int,
    folds: Int, cost: Double, miscalibration: Double)

  case class MiscalibrationLabel(
    qhts: String, label: String, splitSeed: Int, heightStart: Int, heightEnd: Int,
    folds: Int, cost: Double, miscalibration: Double)

}

