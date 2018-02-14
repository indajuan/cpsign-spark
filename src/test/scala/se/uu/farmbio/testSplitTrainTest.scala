package se.uu.farmbio.cpsignspark

import org.scalatest.BeforeAndAfterAll

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

import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import org.apache.spark.sql.execution.datasources.text.TextFileFormat

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class testSplitTrainTest extends FunSuite {

  test("	correct split is done if output file and benchmark file are the same") {
    SplitSDFsTrainTest.main(Array("src/test/resources/input", "src/test/resources/250TrainTest", "0.8", "250", "local", "none"))

    val conf = new SparkConf()
      .setAppName("testSplitTrainTest")
      .setMaster("local")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val spark = SparkSession
      .builder()
      .appName("testSplitTrainTest")
      .config("", "")
      .getOrCreate()

    import spark.implicits._

    val seed250 = spark.read.json("src/test/resources/seed250.json").as[DSTrTe]
      .map(ds => (ds.fileName,ds
          .Train
          .split("\\n").map(z => if (z.contains("CDK  ")) "" else z)
        .mkString("\n").split("\\$\\$\\$\\$").map(z => z.split("\\n").take(2).mkString("")).mkString(", "),
          ds
          .Test.split("\\n").map(z => if (z.contains("CDK  ")) "" else z)
        .mkString("\n").split("\\$\\$\\$\\$").map(z => z.split("\\n").take(2).mkString("")).mkString(", ")))
        
    val t250 = spark.read.json("src/test/resources/250TrainTest/*.json").as[DSTrTe]
      .map(ds => (ds.fileName,ds
          .Train
          .split("\\n").map(z => if (z.contains("CDK  ")) "" else z)
        .mkString("\n").split("\\$\\$\\$\\$").map(z => z.split("\\n").take(2).mkString("")).mkString(", "),
          ds
          .Test.split("\\n").map(z => if (z.contains("CDK  ")) "" else z)
        .mkString("\n").split("\\$\\$\\$\\$").map(z => z.split("\\n").take(2).mkString("")).mkString(", ")))

    
    println("\n Seed 250:\n")
    seed250.show(4,false)
    println("\n Seed 250 file generated:\n")
    t250.show(4,false)

    
    val diff250 = seed250.except(t250).union(t250.except(seed250)).count()
    println("\nNumber of non matching elements with seed=250:\t" + diff250.toString)

    assert(diff250 === 0)
    sc.stop()
  }

}


