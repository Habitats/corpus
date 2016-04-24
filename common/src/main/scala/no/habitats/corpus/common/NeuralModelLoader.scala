package no.habitats.corpus.common

import java.io._
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.deeplearning4j.nn.conf.MultiLayerConfiguration
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.factory.Nd4j

object NeuralModelLoader {

  def coefficientsPath(label: String, count: Int): String = Config.modelPath + s"coefficients_${label}_${count}.bin"
  def confPath(label: String, count: Int): String =  Config.modelPath + s"conf_${label}_${count}.json"

  // Returns ("sport", <model>) pairs
  lazy val bestModels: Map[String, MultiLayerNetwork] = bestModel("conf_").zip(bestModel("coefficients_"))
    .map { case (conf, coeff) => {
      val name = conf.split("[/\\\\]").last
      val label = name.substring(name.indexOf("_") + 1, name.lastIndexOf("_"))
      (label, load(conf, coeff))
    }
    }.toMap

  def bestModel(prefix: String): Seq[String] = {
    new File(Config.modelPath).listFiles
      .filter(_.getName.startsWith(prefix))
      .map(_.getAbsolutePath)
      .groupBy(n => n.substring(0, n.lastIndexOf("_")))
      .map(_._2.maxBy(n => n.substring(n.lastIndexOf("_"), n.length)))
      .toSeq.distinct
      .sorted
  }

  def exists(label: String, count: Int): Boolean = new File(NeuralModelLoader.confPath(label, 10000)).exists && new File(NeuralModelLoader.coefficientsPath(label, 10000)).exists

  def save(model: MultiLayerNetwork, label: String, count: Int) = {
    // write parameters
    val dos = new DataOutputStream(Files.newOutputStream(Paths.get(coefficientsPath(label, count))))
    Nd4j.write(model.params(), dos)

    // write config
    FileUtils.write(new File(confPath(label, count)), model.getLayerWiseConfigurations.toJson)
    dos.close()
  }

  def load(label: String, count: Int): MultiLayerNetwork = {
    // load config
    val config: String = confPath(label, count)
    val coefficients: String = coefficientsPath(label, count)
    load(config, coefficients)
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
