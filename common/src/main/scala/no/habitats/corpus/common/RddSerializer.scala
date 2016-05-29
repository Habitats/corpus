package no.habitats.corpus.common

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD

trait RddSerializer {

  protected def saveAsText(rdd: RDD[String], name: String, partitions: Int = 1) = {
    val path = Config.cachePath + s"${name.replaceAll("[,\\s+]+", "_")}"
    FileUtils.deleteDirectory(new File(path))
    rdd.coalesce(partitions, shuffle = true).saveAsTextFile(path)
    if (partitions == 1) {
      val file = new File(path + ".txt")
      Files.move(new File(path + "/part-00000").toPath, file.toPath, StandardCopyOption.REPLACE_EXISTING)
      FileUtils.deleteDirectory(new File(path))
    }
  }
}
