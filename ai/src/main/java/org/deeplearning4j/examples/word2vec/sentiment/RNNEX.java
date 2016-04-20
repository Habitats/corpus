package org.deeplearning4j.examples.word2vec.sentiment;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.api.rng.Random;
import org.nd4j.linalg.factory.Nd4j;

import no.habitats.corpus.common.Log;

public class RNNEX {

  public static void main(String... args) {
    int    vectorSize   = 1000;
    int    hiddenNodes  = 333; // should not be less than a quarter of the input size
    double learningRate = 0.05;

    Random   random = Nd4j.getRandom();
    INDArray ret    = Nd4j.create(1000, 1000);
    INDArray linear = ret.linearView();
    System.out.println("Length: "+linear.length());
    for (int i = 0; i < linear.length(); i++) {
      linear.putScalar(i, random.nextGaussian());
    }
    Log.v(ret);

//      MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
//          .seed(0)
//          .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
//          .iterations(1)
//          .updater(Updater.RMSPROP)
//          .regularization(true).l2(1e-5)
//          .weightInit(WeightInit.XAVIER)
//          .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue).gradientNormalizationThreshold(1.0)
//          .learningRate(learningRate)
//          .list(2)
//          .layer(0, new GravesLSTM.Builder()
//              .nIn(vectorSize)
//              .nOut(hiddenNodes)
//              .activation("softsign")
//              .build())
//          .layer(1, new RnnOutputLayer.Builder()
//              .nIn(hiddenNodes)
//              .nOut(2)
//              .activation("softmax")
//              .lossFunction(LossFunctions.LossFunction.MCXENT)
//              .build())
//          .pretrain(false)
//          .backprop(true)
//          .build();
//
//      MultiLayerNetwork net = new MultiLayerNetwork(conf);
//      net.init();
//      net.setListeners(new ScoreIterationListener(1));
  }

}
