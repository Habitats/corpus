package no.habitats.corpus.dl4j

import java.io.File

import org.canova.api.util.ClassPathResource
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors
import org.deeplearning4j.models.word2vec.Word2Vec.Builder
import org.deeplearning4j.plot.BarnesHutTsne
import org.deeplearning4j.text.sentenceiterator.{LineSentenceIterator, SentencePreProcessor}
import org.deeplearning4j.text.tokenization.tokenizer.TokenPreProcess
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.EndingPreProcessor
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object W2V extends App {
  val log = LoggerFactory.getLogger(W2V.getClass)

  def example = {
    log.info("Loading data ...")
    val resource = new ClassPathResource("raw_sentences.txt")
    val iter = new LineSentenceIterator(resource.getFile)
    iter.setPreProcessor(new SentencePreProcessor {
      override def preProcess(sentence: String): String = sentence.toLowerCase
    })

    val preProcessor = new EndingPreProcessor
    val tokenizer = new DefaultTokenizerFactory
    tokenizer.setTokenPreProcessor(new TokenPreProcess {
      override def preProcess(token: String): String = {
        val base = preProcessor.preProcess(token.toLowerCase).replaceAll("\\d", "d")
        if (base.endsWith("ly") || base.endsWith("ing")) println
        base
      }
    })


    log.info("Training model ...")
    val batchSize = 1000
    val iterations = 30
    val layerSize = 300

    val vec = new Builder()
      .batchSize(batchSize) // # words processed at a time
      .sampling(1e-5) // negative sampling
      .minWordFrequency(5) // TFT - term frequency threshold
      .useAdaGrad(false) // Adagard creates a different gradient for each feature
      .layerSize(layerSize) // dimension of feature vector
      .iterations(iterations) // iterations to train
      .learningRate(0.025) // the step size for each update of the coefficients
      .minLearningRate(1e-2) // learning rate decays wrt # words
      .negativeSample(10) // sample size 10
      .iterate(iter)
      .tokenizerFactory(tokenizer)
      .build()
    vec.fit // begin training

    log.info("Evaluating model ...")
    val sim = vec.similarity("people", "money")
    log.info("Similarity between people and money: " + sim)
    val similar = vec.wordsNearest("day", 10)
    log.info("Words nearest 'day': " + similar.asScala.mkString(", "))

    log.info("Plot TSNE ...")
    val tsne = new BarnesHutTsne.Builder()
      .setMaxIter(1000)
      .stopLyingIteration(250)
      .learningRate(500)
      .useAdaGrad(false)
      .theta(0.5)
      .setMomentum(0.5)
      .normalize(true)
      .usePca(false)
      .build()
    vec.lookupTable().plotVocab(tsne)

    log.info("Save vectors ...")
    WordVectorSerializer.writeWordVectors(vec, "words.txt")

    // king + woman - queen
    val kingslist = vec.wordsNearest(Seq("king", "woman").asJava, Seq("queen").asJava, 10)

    // reload
    val wordVectors: WordVectors = WordVectorSerializer.loadTxtVectors(new File("words.txt"))

    val weightLookupTable = wordVectors.lookupTable()
    val vectors = weightLookupTable.vectors()
    val wordVector = wordVectors.getWordVectorMatrix("myword")
    val wordVector2 = wordVectors.getWordVector("myword")
  }

  // load google
  val gModel = new File("D:/Archive/w2v/freebase-vectors-skipgram1000-en.bin")
  val gVec = WordVectorSerializer.loadGoogleModel(gModel, true)
}
