package no.habitats.corpus.common.dl4j

import java.io._
import java.nio.file.{Files, Paths}

import no.habitats.corpus.common.{Config, Log}
import org.apache.commons.io.FileUtils
import org.deeplearning4j.nn.conf.MultiLayerConfiguration
import org.deeplearning4j.nn.layers.feedforward.dense.DenseLayer
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.factory.Nd4j

object NeuralModelLoader {

  def coefficientsPath(name: String, label: String, count: Int): String = s"coefficients-${name}_${label}_${count}.bin"
  def confPath(name: String, label: String, count: Int): String = s"conf-${name}_${label}_${count}.json"

  // Returns ("sport", <model>) pairs
  def bestModels(name: String): Map[String, MultiLayerNetwork] = bestModel("conf-" + name).zip(bestModel("coefficients-" + name))
    .map { case (conf, coeff) => {
      val name = conf.split("[/\\\\]").last
      val label = name.substring(name.indexOf("_") + 1, name.lastIndexOf("_"))
      (label, load(conf, coeff))
    }
    }.toMap

  def models(path: String): Map[String, MultiLayerNetwork] = {
    val fileNames = new File(Config.modelPath + path).listFiles().map(_.getName).sorted
    val pairs = fileNames.filter(_.startsWith("conf")).zip(fileNames.filter(_.startsWith("coef"))).map { case (conf, coef) => {
      val label = coef.substring(coef.indexOf("_") + 1, coef.lastIndexOf("_"))
      (label, load(s"${Config.modelPath}$path/$conf", s"${Config.modelPath}$path/$coef"))
    }}.toMap
    pairs
  }

  def bestModel(prefix: String): Seq[String] = {
    new File(Config.modelPath).listFiles
      .filter(_.getName.startsWith(prefix))
      .map(_.getAbsolutePath)
      .groupBy(n => n.substring(0, n.lastIndexOf("_")))
      .map(_._2.maxBy(n => n.substring(n.lastIndexOf("_"), n.length)))
      .toSeq.distinct
      .sorted
  }

  def save(model: MultiLayerNetwork, label: String, count: Int) = {
    // write parameters
    val name = if (model.getLayer(0).isInstanceOf[DenseLayer]) "ffa" else "rnn"
    val dos = new DataOutputStream(Files.newOutputStream(Paths.get(Config.cachePath + coefficientsPath(name, label, count))))
    Nd4j.write(model.params(), dos)

    // write config
    FileUtils.write(new File(Config.cachePath + confPath(name, label, count)), model.getLayerWiseConfigurations.toJson)
    dos.close()
  }

  def load(config: String, coefficients: String): MultiLayerNetwork = {
    Log.v(s"Loading ${config} ...")
    val conf = MultiLayerConfiguration.fromJson(FileUtils.readFileToString(new File(config)))

    // load parameters
    Log.v("Loading %s ...".format(coefficients))
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(coefficients)))
    val params = Nd4j.read(dis)
    dis.close()

    // create network
    Log.v("Initializing network ...")
    val model = new MultiLayerNetwork(conf)
    model.init()
    model.setParameters(params)

    model
  }
}
