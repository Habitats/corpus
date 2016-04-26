package no.habitats.corpus.common

import java.io._

import org.apache.spark.mllib.classification.ClassificationModel

object MLlibModelLoader {

  def save(model: ClassificationModel, name: String) = {
    val fos = new FileOutputStream(Config.modelPath + name)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close
  }

  def load(name: String): ClassificationModel = {
    val fos = new FileInputStream(Config.modelPath + name)
    val oos = new ObjectInputStream(fos)
    val newModel = oos.readObject().asInstanceOf[ClassificationModel]
    newModel
  }
}
