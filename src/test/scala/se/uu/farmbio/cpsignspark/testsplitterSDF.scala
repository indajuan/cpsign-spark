package se.uu.farmbio.cpsignspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class testsplitterSDF extends FunSuite{
  
  test("correct split is done if output file and benchmark file are the same"){
    splitterSDF.main(Array("--sparkMaster", "local", "--inputFolder", "src/test/resources/input", 
        "--outputFolder", "src/test/resources/output1/", "--seedInput", "20", "--splitRatio", "0.2", 
        "--swiftOpenstack", "none", "--numberOfSplits", "1"))

    val conf = new SparkConf().setAppName("testsplitterSDF").setMaster("local")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val bench = sc.wholeTextFiles("src/test/resources/splitTest").map(z => (z._1.split("/").last.split("\\.").head, z._2))
    bench.keys.collect().foreach(println)
    val train = sc.wholeTextFiles("src/test/resources/output1").map(z => (z._1.split("/").last.split("\\.").head, z._2))
    val rdd = bench.join(train)
    val diff = rdd.mapValues(z => z._1!=z._2).values.map(z => (if(z) 1 else 0)).reduce((a, b) => a + b)
    
    println("\nNumber of non matching elements:\t" + diff.toString)
    assert(diff === 0)
    sc.stop()
  }
  
}


