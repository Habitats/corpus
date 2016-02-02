package no.habitats.corpus.hbase

import com.google.BigTableHelper
import no.habitats.corpus.Log
import no.habitats.corpus.hbase.HBaseConstants._
import no.habitats.corpus.hbase.Implicits._
import no.habitats.corpus.models.{Annotation, Article}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}

import scala.collection.JavaConverters._

object HBaseUtil {

  def conf: Configuration = {
    val conf = BigTableHelper.conf()
    conf.set(TableInputFormat.INPUT_TABLE, articlesId)
    conf
  }

  def conn: Connection = BigTableHelper.connect(conf)

  def admin: Admin = conn.getAdmin

  def hbaseTest() = {
    Log.i("Checking HBase availability ...")
    try {
      admin.listTables()
      Log.i("HBase is up!")
    } catch {
      case e: Exception => Log.e(e.printStackTrace())
    }
  }

  def init() = {
    if (!admin.isTableAvailable(articlesId)) {
      Log.v("Creating " + articlesId + " table")
      val articleTable = new HTableDescriptor(articlesId)
      articleTable.addFamily(new HColumnDescriptor(nyFamily))
      articleTable.addFamily(new HColumnDescriptor(googleFamily))
      admin.createTable(articleTable)
    } else {
      Log.i("Table already exists!")
    }
  }

  def drop() = {
    admin.disableTable(articlesId)
    admin.deleteTable(articlesId)
    Log.i("Dropped table: " + articlesId)
  }

  def list() = {
    val descriptors = admin.listTableNames.map(_.getNameAsString)
    Log.i("Tables: " + descriptors)
  }

  def disable() = {
    if (!admin.isTableDisabled(articlesId)) {
      admin.disableTable(articlesId)
      Log.i("Disabled: " + articlesId)
    } else {
      Log.i("Table " + articlesId + " already disabled")
    }
  }

  def enable() = {
    if (!admin.isTableEnabled(articlesId)) {
      admin.enableTable(articlesId)
      Log.i("Enabled: " + articlesId)
    } else {
      Log.i("Table " + articlesId + " already enabled")
    }
  }

  def add(name: String) = {
    admin.addColumn(articlesId, new HColumnDescriptor(name))
  }

  def add(articles: Seq[Article]) = {
    val table = conn.getTable(articlesId)
    table.batch(articles.map(put).asJava)
    table.close()
  }

  def add(article: Article) = {
    val table = conn.getTable(articlesId)
    table.put(put(article))
    table.close()
  }

  def put(article: Article): Put = {
    val p = new Put(article.id)
    p.addColumn(nyFamily, id, article.id)
    p.addColumn(nyFamily, headline, article.hl)
    p.addColumn(nyFamily, wordCount, article.wc)
    p.addColumn(nyFamily, date, article.date)
    p.addColumn(nyFamily, predictions, article.pred)
    p.addColumn(nyFamily, iptc, article.iptc)
    p.addColumn(nyFamily, url, article.url)

    p.addColumn(googleFamily, annotationCount, article.ann.size)
    article.ann.values.foreach(ann => {
      val c = article.id + ":" + ann.index + ":"
      p.addColumn(googleFamily, c + id, ann.articleId)
      p.addColumn(googleFamily, c + annotationIndex, ann.index)
      p.addColumn(googleFamily, c + phrase, ann.phrase)
      p.addColumn(googleFamily, c + mentionCount, ann.mc)
      p.addColumn(googleFamily, c + offset, ann.offset)
      p.addColumn(googleFamily, c + freeBaseId, ann.fb)
      p.addColumn(googleFamily, c + tfIdf, ann.tfIdf)
    })
    p
  }

  def toArticle(res: Result): Article = {
    val annCount: Int = res.getValue(googleFamily, annotationCount)
    val annotations = (0 until annCount.toInt).map(index => {
      val articleId: String = res.getRow
      val c = articleId + ":" + index + ":"
      Annotation(
        articleId = res.getValue(googleFamily, c + id),
        index = res.getValue(googleFamily, c + annotationIndex),
        phrase = res.getValue(googleFamily, c + phrase),
        mc = res.getValue(googleFamily, c + mentionCount),
        offset = res.getValue(googleFamily, c + offset),
        fb = res.getValue(googleFamily, c + freeBaseId),
        wd = res.getValue(googleFamily, c + wikidataId),
        tfIdf = res.getValue(googleFamily, c + tfIdf)
      )
    }).map(a => (a.fb, a)).toMap

    val article = Article(
      id = res.getRow,
      hl = res.getValue(nyFamily, headline),
      wc = res.getValue(nyFamily, wordCount),
      date = res.getValue(nyFamily, date),
      iptc = res.getValue(nyFamily, iptc),
      url = res.getValue(nyFamily, url),
      pred = res.getValue(nyFamily, predictions),
      ann = annotations
    )

    article
  }
}