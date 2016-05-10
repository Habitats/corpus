/**
  * Created by Patrick on 13.11.2015.
  */

import java.io.File

import no.habitats.corpus.common.{Config, CorpusContext}
import org.junit.runner.RunWith
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkINDArrayTest extends FunSuite {

  test("INDArray in RDD") {
    val floatsPath: String = Config.testPath + "1000d_vectors_small.txt"
    assert(new File(floatsPath).exists)
    val rdd = CorpusContext.sc.textFile(floatsPath, 100)
      .map(_.split(",").map(_.toFloat))
      .map(Nd4j.create)
      .zipWithIndex()

    val arr = rdd.collect

    assert(arr.forall(_._1.size(1) == 1000))
    assert(rdd.keys.filter(_.size(1) == 1000).count == rdd.count)
    assert(arr.size == rdd.count)
  }

  test("ND4j") {
    val vectors: Array[INDArray] = Array.fill[INDArray](10000)(Nd4j.create(Array.fill[Double](10000)(Math.random)))
    var last = System.currentTimeMillis()
    val combined1: INDArray = vectors.reduce(_.add(_)).div(vectors.size)
    println(System.currentTimeMillis() - last)
    last = System.currentTimeMillis()
    val combined2: INDArray = vectors.reduce(_.addi(_)).divi(vectors.size)
    println(System.currentTimeMillis() - last)
    last = System.currentTimeMillis()
    val combined3: INDArray = Nd4j.hstack(vectors: _*).mean(0) // supposed to be fast, but it's 3 times slower
    println(System.currentTimeMillis() - last)
    assert(combined1 == combined2)
    assert(combined2 == combined3)
  }
}
