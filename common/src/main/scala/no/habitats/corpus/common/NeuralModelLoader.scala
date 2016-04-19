package no.habitats.corpus.common

import java.io.{DataInputStream, DataOutputStream, File, FileInputStream}
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.deeplearning4j.nn.conf.MultiLayerConfiguration
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.factory.Nd4j

object NeuralModelLoader {

  def coefficientsPath(label: String, count: Int): String = Config.cachePath + s"coefficients_${label}_${Config.count}.bin"
  def confPath(label: String, count: Int): String = Config.cachePath + s"conf_${label}_${Config.count}.json"

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

    Log.v(s"Loading ${config} ...")
    val conf = MultiLayerConfiguration.fromJson(FileUtils.readFileToString(new File(config)))

    // load parameters
    Log.v("Loading %s ...".format(coefficients))
    val dis = new DataInputStream(new FileInputStream(coefficients))
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
