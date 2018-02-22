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

case class CP(HS:Int, HE:Int, Fold:Int, Cost:Int, File:String, CDK:String, Label:String, P0:Double, P1:Double)

object CCP {

  def main(args: Array[String]) {
    //    val inputFolder = args(0)
    //    val outputFolder = args(1)
    //    val splitRatio = args(2).toFloat
    //    val seedInput = args(3).toInt
    //    val sparkmaster = args(4)
    //    val swiftOpenstack = args(5)
    //    val numberOfSplits = args(6).toInt

    //ARGS
    val inputFolder = "src/test/resources/input"
    val outputFolder = "src/test/resources/output"
    val splitRatio = 0.8f
    val seedInput = 250
    val sparkmaster = "local"
    val swiftOpenstack = "none"
    val numberOfSplits = 1
    val heightStart = Array(1).toList.map(_.toString.toInt)
    val heightEnd = Array(3).map(_.toString.toInt)
    val folds = Array(2).map(_.toString.toInt)
    val costs = Array(50).map(_.toString.toInt)

    implicit class Crossable[X](xs: List[X]) {
      def cross[Y](ys: Array[Y]) = for { x <- xs; y <- ys } yield (x, y)
    }

    val parCpsign = ((heightStart cross heightEnd cross folds)
      .map(z => (z._1._1, z._1._2, z._2)) cross costs)
      .map(z => (z._1._1, z._1._2, z._1._3, z._2)).map(z => z.productIterator.toList.mkString("\n"))

    //SPARK CONF
    val conf = new SparkConf().setAppName("ccpScala")
    if (sparkmaster == "local") conf.setMaster("local")
    val sc = new SparkContext(conf)
    if (swiftOpenstack != "none") sc.addJar(swiftOpenstack)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder().appName("CCPsparkSession").config("", "").getOrCreate()
    import spark.implicits._

    //READ DATA
    val wholeSDFs = new filesSDF(sc.wholeTextFiles(inputFolder, numberOfSplits))
    
    //MAKE RDD[STRING]
    val dataSet0 = wholeSDFs
      .toListString(seedInput, splitRatio)
      .map(t => (t._1.split("\\.").head + t._2.mkString("\nTESTSDFFILE")))
    
    //DATASETS
    println("\nDatasets:")
    dataSet0.map(z => z.split("\n").head).collect().foreach(println)

    for (p <- parCpsign) {
      println("\nParameters:\n"+ p + "\n")
      val dataSet1 = dataSet0.map(z => p + "\n" + z)
      val predictions = new EasyMapReduce(dataSet1)
        .setInputMountPoint("/dataSet.txt")
        .setOutputMountPoint("/out.txt")
        // Train
        .map(
          imageName = "indajuan/cpsign",
          command = // SPLIT STRING INTO DATASET, TRAIN, TEST
            "head -n 5 /dataSet.txt  > pars && " + // save parameters
              "export heightStart=$(head -1 pars | tr -d \"\n\") && " +
              "export heightEnd=$(tail -n+2 pars | head -n 1 | tr -d \"\n\") && " +
              "export folds=$(tail -n+3 pars | head -n 1 | tr -d \"\n\") && " +
              "export costs=$(tail -n+4 pars | head -n 1 | tr -d \"\n\") && " +
              "export dataName=$(tail -n+5 pars | head -n 1 | tr -d \"\n\") && " +
              "tail -n+6 /dataSet.txt  > /dataSDF.txt && " + // remove the dataset name
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
          val pv0 = compact(render(parsedJson \ "prediction" \ "pValues" \ "-1")).toDouble
          val pv1 = compact(render(parsedJson \ "prediction" \ "pValues" \ "1")).toDouble
          val label = compact(render(parsedJson \ "molecule" \ "class"))
          val fileN = compact(render(parsedJson \ "molecule" \ "file"))
          val pa = p.split("\\n").map(_.trim).toList

          CP(pa(0).toInt, pa(1).toInt, pa(2).toInt, pa(3).toInt, fileN, key, label, pv0, pv1)
        }.toDF.write.format("json").mode("overwrite").save(outputFolder+"_"+p.split('\n').map(_.trim.filter(_ >= ' ')).mkString("-"))
    }

  }
}

 // <input dir src/test/resources/input> <output dir <src/test/resources/output>\
// <splitratio 0.8> <seed 250> <master local> <jar added none> <numberOfSplits 1>