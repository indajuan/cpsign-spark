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

import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import org.apache.spark.sql.execution.datasources.text.TextFileFormat

object testSplit extends FunSuite {

  val conf = new SparkConf()
    .setAppName("testSplit")
    .setMaster("local")

  val sc = new SparkContext(conf)
  
  //val sqlContext = new SQLContext(sc)
  //import sqlContext._
  //import sqlContext.implicits._

  val spark = SparkSession
    .builder()
    .appName("testSplit")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
  import spark.implicits._

  test("	correct split is done if output file and benchmark file are the same") {

    SplitSDF.main(Array("src/test/resources/input", "src/test/resources/100", "0.8", "100"))

    SplitSDF.main(Array("src/test/resources/input", "src/test/resources/250", "0.8", "250"))
    
    val seed100 = spark.read.json("src/test/resources/seed100.json").as[DS]

    val seed250 = spark.read.json("src/test/resources/seed250.json").as[DS]

    val t100 = spark.read.json("src/test/resources/100/*.json").as[DS]

    val t250 = spark.read.json("src/test/resources/250/*.json").as[DS]

    assert(seed100.except(t100).union(t100.except(seed100)).count() === 0)
    assert(seed250.except(t250).union(t250.except(seed250)).count() === 0)

  }

}