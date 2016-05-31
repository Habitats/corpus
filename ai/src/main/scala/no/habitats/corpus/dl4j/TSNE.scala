package no.habitats.corpus.dl4j

import java.io.File
import java.util

import no.habitats.corpus.common.models.{Article, CorpusDataset}
import no.habitats.corpus.common.{Config, IPTC, Log, W2VLoader}
import org.apache.spark.rdd.RDD
import org.deeplearning4j.plot.Tsne
import org.nd4j.linalg.api.buffer.DataBuffer._
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.collection.JavaConverters._

/**
  * Created by mail on 28.04.2016.
  */
object TSNE {

  def create(rdd: RDD[Article], name: String = "tsne.csv", useDocumentVectors: Boolean) = {
    val iterations: Int = 200
    Nd4j.dtype = Type.DOUBLE
    Nd4j.factory.setDType(Type.DOUBLE)

    Log.v("Load & Vectorize data....")
    val weights = useDocumentVectors match {
      case true => stackDocumentVectors(rdd)
      case false => stackWordVectors(rdd)
    }

    Log.v("Build model....")
    val tsne = new Tsne.Builder()
      .setMaxIter(iterations)
      //      .theta(0.5)
      .normalize(false)
      .learningRate(500)
      .useAdaGrad(false)
      .usePca(false)
      .build()

    Log.v("Store TSNE Coordinates for Plotting....")
    val outputFile: String = Config.dataPath + name
    new File(outputFile).delete()
    val arr: Array[INDArray] = weights.map(_._2)
    val labels: util.List[String] = weights.map(_._1).map(IPTC.trim).toList.asJava
    //    val matrix = arr.reduce(Nd4j.vstack(_, _))
    val matrix = Nd4j.vstack(arr: _*)
    tsne.plot(matrix, 2, labels, outputFile)

    val values: List[String] = Config.dataFile(outputFile).getLines().map(_.replace(",", "@").replace(".", ",").split("@")).map(lst => {
      val x = lst(0).trim
      val y = lst(1).trim
      val c = lst(2).trim
      Seq(c, x, y).mkString(" ")
    }).toList

    Log.saveToList(values, "tsne.txt")
  }

  def stackDocumentVectors(rdd: RDD[Article]): Array[(String, INDArray)] = {
//    rdd.map(a => (a.iptc, CorpusDataset.documentVector(a,))).flatMap { case (iptc, v) => iptc.map(c => (c, v)) }.collect()
    null
  }

  def stackWordVectors(rdd: RDD[Article]): Array[(String, INDArray)] = {
    rdd
      .flatMap(a => a.ann.map(x => (a.iptc, W2VLoader.fromId(x._2.fb).get)))
      .flatMap { case (iptc, v) => iptc.map(c => (c, v)) }
      .groupBy { case (label, v) => v }
      .map { case (v, pairs) => {
        val maxLabel = pairs.groupBy { case (label, v) => label }.maxBy(_._2.size)._1
        (maxLabel, v)
      }
      }.collect()
  }
}
