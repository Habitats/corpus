package no.habitats.corpus.common.dl4j

import java.io._
import java.nio.file.{Files, Paths}

import no.habitats.corpus.common.{Config, Log}
import org.apache.commons.io.FileUtils
import org.deeplearning4j.nn.conf.MultiLayerConfiguration
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

case class NeuralModel(confg: String, coef: String) {
  lazy val network = NeuralModelLoader.load(confg, coef)
}

object NeuralModelLoader {

  def models(path: String): Map[String, NeuralModel] = {
    val fileNames = new File(path).listFiles().map(_.getName).sorted
    val pairs = fileNames.filter(_.startsWith("conf")).zip(fileNames.filter(_.startsWith("coef"))).map { case (conf, coef) => {
      val label = coef.split("_|-").last.split("\\.").head // fetch "society" from "coefficients-confidence-25_ffn_w2v_all_society.bin"
      (label, NeuralModel(s"${path}/$conf", s"${path}/$coef"))
    }
    }.toMap
    pairs
  }

  def model(name: String, tag: String, label: String): String = {
    new File(Config.modelDir(name, tag)).listFiles().find(n => n.getName.contains(label) && n.getName.endsWith(".bin")).map(_.getAbsolutePath).getOrElse(throw new IllegalArgumentException(s"No such model: $name with $tag for $label!"))
  }

  def save(model: MultiLayerNetwork, label: String, name: String, tag: String) = {
    val coefficientsPath = Config.modelDir(name, tag) + s"coefficients-${name}_${label}.bin"
    val confPath = Config.modelDir(name, tag) + s"conf-${name}_${label}.json"

    // write parameters
    new File(coefficientsPath).getParentFile.mkdirs()
    val dos = new DataOutputStream(Files.newOutputStream(Paths.get(coefficientsPath)))
    Nd4j.write(model.params(), dos)

    // write config
    FileUtils.write(new File(confPath), model.getLayerWiseConfigurations.toJson)
    dos.close()
    Log.v(s"Successfully saved model $name - $label ...")
  }

  def load(config: String, coefficients: String): MultiLayerNetwork = {
    val conf: MultiLayerConfiguration = loadConfig(config)
    // load parameters
    val params: INDArray = loadCoefficients(coefficients)
    // create network
    createModel(conf, params)
  }

  def load(conf: MultiLayerConfiguration, coefficients: String): MultiLayerNetwork = {
    createModel(conf, loadCoefficients(coefficients))
  }

  def createModel(conf: MultiLayerConfiguration, coefficients: INDArray): MultiLayerNetwork = {
    Log.v("Initializing network ...")
    val model = new MultiLayerNetwork(conf)
    model.init()
    model.setParameters(coefficients)

    Log.v("Successfully loaded model.")
    model
  }

  def loadConfig(path: String): MultiLayerConfiguration = {
    Log.v(s"Loading ${path} ...")
    val conf = MultiLayerConfiguration.fromJson(FileUtils.readFileToString(new File(path)))
    conf
  }

  def loadCoefficients(path: String): INDArray = {
    Log.v("Loading %s ...".format(path))
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))
    val params = Nd4j.read(dis)
    dis.close()
    params
  }
}
