package se.uu.farmbio.cpsignspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import scala.util.Random

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import se.uu.it.mare.MaRe
import java.io.File
import scopt.OptionParser
import se.uu.farmbio.cpsignspark.fcns._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray
import org.apache.spark.RangePartitioner

case class CP(qhts: String, molID: String, label: String,
    splitSeed: Int, heightStart: Int, heightEnd: Int, 
    folds: Int, cost: Double,  p0: Double, p1: Double)

object gridCrossConformalPrediction {

  case class Arglist(
    sparkMaster:    String      = "local",
    javaMemoryForContainer: String  = "2048M",
    inputFile:      String      = "",
    outputFolder:   String      = "",
    swiftOpenstack: String      = "none",
    seeds:          Seq[Int]    = Seq(10),
    heightStart:    Seq[Int]    = Seq(1),
    heightEnd:      Seq[Int]    = Seq(3),
    folds:          Seq[Int]    = Seq(5),
    costs:          Seq[Double] = Seq(50),
    stratified:     Boolean      = false,
    cpsignSeed:     String         = " ")

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("CCP") {
      head("Cross Conformal Prediction grid prediction.")
      opt[String]("sparkMaster")
        .text("SparkMaster")
        .action((x, c) => c.copy(sparkMaster = x))
      opt[Boolean]("stratified")
        .text("stratified")
        .action((x, c) => c.copy(stratified = x))
      opt[String]("cpsignSeed")
        .text("cpsignSeed")
        .action((x, c) => c.copy(cpsignSeed = x))
      opt[String]("javaMemoryForContainer")
        .text("javaMemoryForContainer")
        .action((x, c) => c.copy(javaMemoryForContainer = x))
      opt[String]("inputFile")
        .required()
        .validate(x =>
          if (List("aid411", "aid1030", "aid1721", "aid2326", "aid2451", "aid485290", "aid485314", "aid504444").contains(x.toLowerCase())) success
          else failure("Options aid411, aid1030, aid1721, aid2326, aid2451, aid485290, aid485314, aid504444"))
        .text("NameOfDatasets/DockerContainer")
        .action((x, c) => c.copy(inputFile = x))
      opt[String]("outputFolder")
        .required()
        .text("PathToOutputPredictionfiles")
        .action((x, c) => c.copy(outputFolder = x))
      opt[String]("swiftOpenstack")
        .text("SwiftAddressForObjectStorage")
        .action((x, c) => c.copy(swiftOpenstack = x))
      opt[Seq[Int]]("seeds")
        .validate(x =>
          if (x.toSet.subsetOf(Set(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200))) success
          else failure("Options 10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200"))
        .text("seedsOfRandomSplitsInDockerContainer")
        .action((x, c) => c.copy(seeds = x))
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
      opt[Seq[Double]]("costs")
        .validate(x =>
          if (x.min >= 0) success
          else failure("Option must be >0"))
        .text("CostForCCP")
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
    val inputFile = params.inputFile
    val javaMemoryForContainer = params.javaMemoryForContainer
    val outputFolder = params.outputFolder
    val sparkMaster = params.sparkMaster
    val swiftOpenstack = params.swiftOpenstack
    val seeds = params.seeds.toList
    val heightStart = params.heightStart.toList
    val heightEnd = params.heightEnd.toList
    val folds = params.folds.toList
    val costs = params.costs.toList
    val stratified = params.stratified
    val cpsignSeed = params.cpsignSeed

    //SPARK CONF
    val conf = new SparkConf().setAppName("gridCCP-liblinear")
    if (sparkMaster == "local") conf.setMaster("local")
    val sc = new SparkContext(conf)
    if (swiftOpenstack != "none") sc.addJar(swiftOpenstack)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder().appName("gridCCP-liblinear").config("", "").getOrCreate()
    import spark.implicits._

    val models0 = (for (
      s <- seeds;
      hs <- heightStart;
      he <- heightEnd;
      f <- folds;
      c <- costs
    ) yield (inputFile.toUpperCase() + "_" + s + "-" + hs + "_" + he + "_" + f + "_" + c, hs, he))
    
    val strat = if(stratified) "--stratified " else " "
    val cpSeed = if(cpsignSeed != " ") ("--seed " + cpsignSeed + " ") else cpsignSeed
    val models = models0.filter(z => z._2 <= z._3).distinct.map(z=>z._1 + "\n" + "-Xmx" + javaMemoryForContainer + "\n" + strat + "\n" + cpSeed)
    
    println("\nCross Conformal Prediction for: " + inputFile + " using SVM liblinear CPsign package")
    println("\nNumber of parameter combinations:  " + models.length)
    println("\nJava memory for container:  " + javaMemoryForContainer)

    val rddModels = sc.parallelize(models, models.length)
    val predictions = new MaRe(rddModels)
      .setInputMountPoint("/dataSet.txt")
      .setOutputMountPoint("/out.txt")
      // Train
      .map(
        imageName = "indajuan/" + inputFile.toLowerCase(),
        command = 
          // Parameters
          "echo 'running' && " +
          "export pars0=`head -n 1 /dataSet.txt | tr -d \"\n\"` && " +
          "export fileSDF=`head -n 1 /dataSet.txt | awk '{split($0,a,\"-\"); print a[1]}' | tr -d \"\n\"` && " +
            "export extFileTrain=\"_trn.sdf\" && " +
            "export dataset=`head -n 1 /dataSet.txt | awk '{split($0,a,\"-\"); print a[1]}' | awk -F\"_\"  '{print $1}' | tr -d \"\n\"`  && " +
            "export extFileTest=\"_tst.sdf\" && " +
            "export fileToTrain=$fileSDF$extFileTrain  && " +
            "export fileToTest=$fileSDF$extFileTest  && " +
            "export zipExtention=\".zip\" && " +
            
            "echo 'unzipping sdf files' && " +
            "echo $dataset$zipExtention && " +
            "echo $fileToTrain && " +
            "echo $fileToTest && " +
            "unzip -p $dataset$zipExtention $fileToTrain > $fileToTrain && " +
            "unzip -p $dataset$zipExtention $fileToTest > $fileToTest && " +
            
            "echo 'exporting parameters' && " +
            "export pars=`head -n 1 /dataSet.txt | awk '{split($0,a,\"-\"); print a[2]}' | tr -d \"\n\"` && " +
            "export heightStart=`echo $pars | awk -F\"_\"  '{print $1}' | tr -d \"\n\"` && " +
            "export heightEnd=`echo $pars | awk -F\"_\"  '{print $2}' | tr -d \"\n\"` && " +
            "export fold=`echo $pars | awk -F\"_\"  '{print $3}' | tr -d \"\n\"` && " +
            "export cost=`echo $pars | awk -F\"_\"  '{print $4}' | tr -d \"\n\"` && " +
             
            "echo 'entering parameters in sdf files' && " +
            "sed -i \'s/\\$\\$\\$\\$/>  <par>\\n'\"$pars0\"'\\n\\$\\$\\$\\$/g\' $fileToTest && " +
            "sed -i \'s/\\$\\$\\$\\$/>  <par>\\n'\"$pars0\"'\\n\\$\\$\\$\\$/g\' $fileToTrain && " +
            
            //"export javaMemoryForContainer=`tail -n+2 /dataSet.txt | head -n1 | tr -d \"\n\"` && " +
            //"export stratified=`tail -n+3 /dataSet.txt | head -n1 | tr -d \"\n\"` && " +
            //"export cpSeed=`tail -1 /dataSet.txt | tr -d \"\n\"` && " +
            
          // train  
            "echo 'training' && " +
            "java -Xmx" + javaMemoryForContainer + " -jar cpsign-0.6.6.jar train -t $fileToTrain " + // train file
            "-mn out -mo /model.cpsign -rn class -i liblinear -l [\"-1\",\"1\"] " +
            "-c 3 -hs $heightStart -he $heightEnd -nr $fold --cost $cost " + 
            strat + cpSeed +
            "--percentiles 0 " +
            "--license cpsign0.6-standard.license && " +

            // predict
            "echo 'predicting' && " +
            "java -Xmx" + javaMemoryForContainer + " -jar cpsign-0.6.6.jar predict -c 3 -m /model.cpsign -p $fileToTest -o /out.txt " +
            "--license cpsign0.6-standard.license ")
      .getRDD

    val predictions1 = predictions.map {
      json =>
        val parsedJson = parse(json)
        val key = compact(render(parsedJson \ "molecule" \ "cdk:Title")).replace("\"", "")
        val p0 = compact(render(parsedJson \ "prediction" \ "pValues" \ "-1")).toDouble
        val p1 = compact(render(parsedJson \ "prediction" \ "pValues" \ "1")).toDouble
        val label = compact(render(parsedJson \ "molecule" \ "class")).replace("\"", "")
        val name = compact(render(parsedJson \ "molecule" \ "par")).replace("\"", "")
        val filename = name.split("-")(0).split("_")(0).replace("\"", "")
        val hs = name.split("-")(1).split("_")(0).toInt
        val he = name.split("-")(1).split("_")(1).toInt
        val fold = name.split("-")(1).split("_")(2).toInt
        val cost = name.split("-")(1).split("_")(3).toDouble
        val seed = name.split("-")(0).split("_")(1).toInt
        CP(filename, key, label, seed, hs, he, fold, cost, p0, p1)
    }
    
    val predictionsDF = predictions1.toDF
    
    predictionsDF.repartition(folds.length*seeds.length).write.format("csv").option("header", "true").mode("overwrite").save(outputFolder)
    println("\nSaved: " + outputFolder)
    
    sc.stop()
  }

}