package no.habitats.corpus.common

import java.io._

import org.apache.spark.mllib.classification.{ClassificationModel, NaiveBayesModel}

object MLlibModelLoader {

  def save(model: NaiveBayesModel, name: String) = {
    val fos = new FileOutputStream(Config.cachePath + name)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close
  }

  def load(name: String): NaiveBayesModel = {
    val fos = new FileInputStream(Config.modelPath + name)
    val oos = new ObjectInputStream(fos)
    val newModel = oos.readObject().asInstanceOf[NaiveBayesModel]
    newModel
  }
}
