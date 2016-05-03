package no.habitats.corpus.common

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.models.Article
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD

import scala.collection.Map

trait RddSerializer {


  protected def saveAsText(rdd: RDD[String], name: String) = {
    val path = Config.cachePath + s"${name.replaceAll("[,\\s+]+", "_")}"
    FileUtils.deleteDirectory(new File(path))
    rdd.coalesce(1, shuffle = true).saveAsTextFile(path)
    val file = new File(path + ".json")
    Files.move(new File(path + "/part-00000").toPath, file.toPath, StandardCopyOption.REPLACE_EXISTING)
    FileUtils.deleteDirectory(new File(path))
  }

}
