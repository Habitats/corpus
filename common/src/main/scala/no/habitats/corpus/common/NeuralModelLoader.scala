package no.habitats.corpus.common

import java.io.{DataInputStream, DataOutputStream, File, FileInputStream}
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.deeplearning4j.nn.conf.MultiLayerConfiguration
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.factory.Nd4j

object NeuralModelLoader {
  val coefficientsPath = Config.cachePath + "coefficients.bin"
  val confPath = Config.cachePath + "conf.json"

  lazy val cachedModel = load()

  def save(model: MultiLayerNetwork) = {
    // write parameters
    val dos = new DataOutputStream(Files.newOutputStream(Paths.get(coefficientsPath)))
    Nd4j.write(model.params(), dos)

    // write config
    FileUtils.write(new File(confPath), model.getLayerWiseConfigurations.toJson)
  }

  def load(): MultiLayerNetwork = {
    // load config
    Log.v(s"Loading $confPath ...")
    val conf = MultiLayerConfiguration.fromJson(FileUtils.readFileToString(new File(confPath)))

    // load parameters
    Log.v("Loading %s ...".format(coefficientsPath))
    val dis = new DataInputStream(new FileInputStream(coefficientsPath))
    val params = Nd4j.read(dis)

    // create network
    Log.v("Initializing network ...")
    val model = new MultiLayerNetwork(conf)
    model.init()
    model.setParameters(params)

    model
  }
}
