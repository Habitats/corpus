package no.habitats.corpus.common.mllib

import java.io._

import no.habitats.corpus.common.Config
import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.classification.NaiveBayesModel

object MLlibModelLoader {

  def save(model: NaiveBayesModel, name: String) = {
    val f = new File(name)
    FileUtils.deleteQuietly(f)
    f.getParentFile.mkdirs()
    val fos = new FileOutputStream(f)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close
  }

  def load(path: String, label: String): NaiveBayesModel = {
    val file = new File(path).listFiles().find(_.getName.contains(label)).get
    val fos = new FileInputStream(file)
    val oos = new ObjectInputStream(fos)
    val newModel = oos.readObject().asInstanceOf[NaiveBayesModel]
    newModel
  }
}
