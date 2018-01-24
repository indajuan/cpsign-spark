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
class testSplit extends FunSuite {

  test("	correct split is done if output file and benchmark file are the same") {
    SplitSDF.main(Array("src/test/resources/input", "src/test/resources/100", "0.8", "100"))
    SplitSDF.main(Array("src/test/resources/input", "src/test/resources/250", "0.8", "250"))

    val conf = new SparkConf()
      .setAppName("testSplit")
      .setMaster("local")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val spark = SparkSession
      .builder()
      .appName("testSplit")
      .config("", "")
      .getOrCreate()

    import spark.implicits._

    val seed100 = spark.read.json("src/test/resources/seed100.json").as[DS]
      .map(ds => (ds.fileName.split("/test/resources/input/")(1),ds.isTrain,ds.data.split("\\n").map(z => if (z.contains("CDK  ")) "" else z)
        .mkString("\n").split("\\$\\$\\$\\$").map(z => z.split("\\n").take(2).mkString("")).mkString(", ")))
    val seed250 = spark.read.json("src/test/resources/seed250.json").as[DS]
      .map(ds => (ds.fileName.split("/test/resources/input/")(1),ds.isTrain,ds.data.split("\\n").map(z => if (z.contains("CDK  ")) "" else z)
        .mkString("\n").split("\\$\\$\\$\\$").map(z => z.split("\\n").take(2).mkString("")).mkString(", ")))
    val t100 = spark.read.json("src/test/resources/100/*.json").as[DS]
      .map(ds => (ds.fileName.split("/test/resources/input/")(1),ds.isTrain,ds.data.split("\\n").map(z => if (z.contains("CDK  ")) "" else z)
        .mkString("\n").split("\\$\\$\\$\\$").map(z => z.split("\\n").take(2).mkString("")).mkString(", ")))
    val t250 = spark.read.json("src/test/resources/250/*.json").as[DS]
      .map(ds => (ds.fileName.split("/test/resources/input/")(1),ds.isTrain,ds.data.split("\\n").map(z => if (z.contains("CDK  ")) "" else z)
        .mkString("\n").split("\\$\\$\\$\\$").map(z => z.split("\\n").take(2).mkString("")).mkString(", ")))

    println("\n Seed 100:\n")
    seed100.show(4,false)
    println("\n Seed 100 file generated:\n")
    t100.show(4,false)
    println("\n Seed 250:\n")
    seed250.show(4,false)
    println("\n Seed 250 file generated:\n")
    t250.show(4,false)

    val diff100 = seed100.except(t100).union(t100.except(seed100)).count()
    println("\nNumber of non matching elements with seed=100:\t" + diff100.toString)
    val diff250 = seed250.except(t250).union(t250.except(seed250)).count()
    println("\nNumber of non matching elements with seed=250:\t" + diff250.toString)

    assert(diff100 === 0)
    assert(diff250 === 0)
    sc.stop()
  }

}

