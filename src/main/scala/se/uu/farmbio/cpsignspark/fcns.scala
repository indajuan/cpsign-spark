package se.uu.farmbio.cpsignspark

import java.io._
import java.io.ByteArrayInputStream
import java.io.StringWriter
import java.nio.charset.Charset

import scala.collection.JavaConversions.asJavaIterator
import scala.util.Random

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.openscience.cdk.silent.ChemFile
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.io.MDLV2000Reader
import org.openscience.cdk.io.SDFWriter
import org.openscience.cdk.tools.manipulator.ChemFileManipulator
import collection.JavaConverters._
import org.apache.spark.SparkContext


object fcns {

  implicit class Crossable[X](xs: List[X]) {
    def cross[Y](ys: List[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  
      def splitByLabel(rdd: RDD[(String, String)]): RDD[(String,(List[String], List[String]))] =
    rdd.map {
      case (file, sdfs) => {
        val fileName = file.split("/").last.split("\\.").head
        val molecules = sdfs.replaceAll("\\$\\$\\$\\$\n\n", "\\$\\$\\$\\$\n")
          .replaceAll("\\$\\$\\$\\$\n", "\\$\\$\\$\\$")
          .split("\\$\\$\\$\\$")
          .filter(z => z.contains("class"))
        val label1 = molecules.filter(_.contains(">  <class>\n1")).toList
        val label0 = molecules.filter(_.contains(">  <class>\n-1")).toList
        (fileName,(label1, label0))
      }
    }
    
    def splitNameTestTrain(rdd: RDD[(String, (List[String], List[String]))], seed:List[Int], splitRatio: Float, numberOfSplits:Int) = {
    rdd.flatMap {
      case (fname,(label1, label0)) =>
        seed.map { s =>
        Random.setSeed(s)
        val (trainLabel1, testLabel1) = Random.shuffle(label1).splitAt(Math.ceil(splitRatio * label1.length).toInt)
        val (trainLabel0, testLabel0) = Random.shuffle(label0).splitAt(Math.ceil(splitRatio * label0.length).toInt)
        val train = (trainLabel1 ++ trainLabel0).map(z => z +"\n$$$$\n")
        val test = (testLabel1 ++ testLabel0).map(z => z + "\n$$$$\n")
        (fname + "_" + s.toString, test ,  train)
        }
    }.repartition(numberOfSplits)
  }    
   
     
  /////// CDK
  
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def rddAtomContainer(rdd: RDD[(String, String)]) = {
    rdd.map {
      case (file, sdfs) => {
        val fileName = file.split("/").last.split("\\.").head
        val sdfByteArray = sdfs.getBytes(Charset.forName("UTF-8"))
        val sdfIS = new ByteArrayInputStream(sdfByteArray)
        val reader = new MDLV2000Reader(sdfIS)
        val chemFile = reader.read(new ChemFile)
        val mols = ChemFileManipulator.getAllAtomContainers(chemFile)
        reader.close
        (fileName,mols.asScala.toList)
        //addProperty(mols,"fileName",fileName)
      }
    }
  }

  def splitListIAtomContainer(rdd:RDD[(String, List[IAtomContainer])], seed:Int, splitRatio:Float) = {
    rdd.map{
      case (f,lst) => {
        val posMols = lst.filter(_.getProperty("class") == "1")
        val negMols = lst.filter(_.getProperty("class") == "-1")
        Random.setSeed(seed)
        val (posTrain, posTest) = Random.shuffle(posMols)
            .splitAt(Math.ceil(posMols.length * splitRatio).toInt)
        val (negTrain, negTest) = Random.shuffle(negMols)
            .splitAt(Math.ceil(negMols.length * splitRatio).toInt)
        (f + "_" + seed.toString, posTest ++ negTest, posTrain ++ negTrain)    
      }
    }
  } 


  def addProperty(mols: List[IAtomContainer], property: String, propertyValue: String) = {
    val molsIt = mols.iterator
    while (molsIt.hasNext()) {
      val mol = molsIt.next
      mol.setProperty(property, propertyValue)
    }
    molsIt.toList
  }

  def toSDF(mols: List[IAtomContainer],newProperty:String,newPropertyVal:String,newProp:Boolean): String = {
    val strWriter = new StringWriter()
    val writer = new SDFWriter(strWriter)
    val molsIt = mols.iterator
    while (molsIt.hasNext()) {
      val mol = molsIt.next
      mol.removeProperty("cdk:Remark")
      if(newProp) mol.setProperty(newProperty, newPropertyVal)
      writer.write(mol)
    }
    writer.close
    strWriter.toString()
  }
  
  
  def currentActiveExecutors(sc: SparkContext): List[String] = {
         val allExecutors = sc.getExecutorMemoryStatus.map(_._1)
         val driverHost: String = sc.getConf.get("spark.driver.host")
         allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
       }

  
     
    }



