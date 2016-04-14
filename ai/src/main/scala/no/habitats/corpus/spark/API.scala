package no.habitats.corpus.spark

import no.habitats.corpus.common.CorpusContext
import no.habitats.corpus.common.CorpusContext._
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MultilabelMetrics}
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

object API {

  // all of these: [1.0, 0.0, 3.0]
  val dv = Vectors.dense(1.0, 0.0, 3.0)
  val sv1 = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
  val sv2 = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

  val pos = LabeledPoint(1.0, dv)
  val neg = LabeledPoint(0.0, sv1)

  val dm = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
  val sm = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))

  // RDD[Vector] can be converted to distributed matrices
  val rdd: RDD[Vector] = sc.parallelize(Seq(dv, sv1, sv2))
  val mat = new RowMatrix(rdd)
  val m = mat.numRows()
  val n = mat.numCols()

  val rdd2: RDD[IndexedRow] = sc.parallelize(Seq(IndexedRow(1, dv), IndexedRow(2, sv1), IndexedRow(3, sv2)))
  val mat2 = new IndexedRowMatrix(rdd2)

  val rdd3: RDD[MatrixEntry] = sc.parallelize(Seq(MatrixEntry(0, 1, 0.2), MatrixEntry(0, 2, 0.5), MatrixEntry(0, 5, 0.9)))
  val mat3 = new CoordinateMatrix(rdd3)

  // statistics
  val summary = Statistics.colStats(rdd)
  println(summary.mean)
  println(summary.max)
  println(summary.variance) // and lots more

  // randomization, 1 million values drawn from N(0, 1) evenly distributed in 10 partitions

  import org.apache.spark.mllib.random.RandomRDDs._

  val u = normalRDD(sc, 1000000L, 10)

  //######################
  //### Feature select ###
  //######################

  // TF-IDF
  val docs = Seq(Seq("hello", "world"), Seq("Shoop", "da", "whoop"), Seq("who", "is", "da", "man"))
  val docsRdd = sc.parallelize(docs)
  val hashingTF = new HashingTF()
  val tf: RDD[Vector] = hashingTF.transform(docsRdd)
  tf.cache()
  val idf = new IDF(minDocFreq = 1).fit(tf)
  val tfidf: RDD[Vector] = idf.transform(tf)

  // Word2Vec
  val w2v = new Word2Vec
  val w2vModel = w2v.fit(docsRdd)
  val synonyms = w2vModel.findSynonyms("hello", 20)
  for ((synonym, cosineSimilarity) <- synonyms) {
    println(s"$synonym $cosineSimilarity")
  }

  // scaling
  val scaler = new StandardScaler(withMean = true, withStd = true).fit(tfidf)
  val scaled = tfidf.map(x => scaler.transform(x))

  // normalizer
  val normalizer = new Normalizer(p = Double.PositiveInfinity)
  val normalized = tfidf.map(x => normalizer.transform(x))


  //######################
  //### Classification ###
  //######################

  val labeledRdd: RDD[LabeledPoint] = sc.parallelize(Seq(LabeledPoint(1.0, dv), LabeledPoint(0.0, sv1), LabeledPoint(0.5, sv2)))
  val splits = labeledRdd.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0).cache
  val test = splits(1)

  // SVM classification
  val model = SVMWithSGD.train(training, numIterations = 100)
  // clear the prediction threshold so the model will return probabilities
  model.clearThreshold
  val scoreAndLabels = test.map { point =>
    val score = model.predict(point.features)
    (score, point.label)
  }

  //######################
  //### Evaluation #######
  //######################

  //### Binary evaluation
  val metrics = new BinaryClassificationMetrics(scoreAndLabels)

  metrics.precisionByThreshold.foreach { case (t, p) => println(s"Threshold: $t, Precision: $p") }
  metrics.recallByThreshold.foreach { case (t, r) => println(s"Threshold: $t, Recall: $r") }

  // precision/recall-curve
  val prc = metrics.pr

  metrics.fMeasureByThreshold.foreach { case (t, f) => println(s"Threshold: $t, F-score: $f, Beta = 1") }
  metrics.fMeasureByThreshold(0.5).foreach { case (t, f) => println(s"Threshold: $t, F-score: $f, Beta = 0.5") }

  val auPRC = metrics.areaUnderPR
  val auROC = metrics.areaUnderROC
  println(s"Area under ROC = $auROC, Area under PRC = $auPRC")

  // compute thresholds used in ROC and PR curves
  val thresholds = metrics.precisionByThreshold.map(_._1)

  //### Multilabel evaluation
  val multiScoreAndLabels: RDD[(Array[Double], Array[Double])] = sc.parallelize(
    Seq((Array(0.0, 1.0), Array(0.0, 2.0)),
      (Array(0.0, 2.0), Array(0.0, 1.0)),
      (Array.empty[Double], Array(0.0)),
      (Array(2.0), Array(2.0)),
      (Array(2.0, 0.0), Array(2.0, 0.0)),
      (Array(0.0, 1.0, 2.0), Array(0.0, 1.0)),
      (Array(1.0), Array(1.0, 2.0))), 2)

  val multiMetrics = new MultilabelMetrics(multiScoreAndLabels)
  println(s"Recall: ${multiMetrics.recall}")
  println(s"Precision: ${multiMetrics.precision}")
  println(s"F-score: ${multiMetrics.f1Measure}")
  println(s"Accuracy: ${multiMetrics.accuracy}")
  println(s"Subset accuracy: ${multiMetrics.hammingLoss}")
  multiMetrics.labels.foreach(label => println(s"Class $label precision: ${multiMetrics.precision(label)}"))
}
