package no.habitats.corpus.hbase

import com.google.BigTableHelper
import no.habitats.corpus.Log
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
    conf.set(TableInputFormat.INPUT_TABLE, C.articlesId)
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
    if (!admin.isTableAvailable(C.articlesId)) {
      Log.v("Creating " + C.articlesId + " table")
      val articleTable = new HTableDescriptor(C.articlesId)
      articleTable.addFamily(new HColumnDescriptor(C.nyFamily))
      articleTable.addFamily(new HColumnDescriptor(C.googleFamily))
      admin.createTable(articleTable)
    } else {
      Log.i("Table already exists!")
    }
  }

  def drop() = {
    admin.disableTable(C.articlesId)
    admin.deleteTable(C.articlesId)
    Log.i("Dropped table: " + C.articlesId)
  }

  def list() = {
    val descriptors = admin.listTableNames.map(_.getNameAsString)
    Log.i("Tables: " + descriptors)
  }

  def disable() = {
    if (!admin.isTableDisabled(C.articlesId)) {
      admin.disableTable(C.articlesId)
      Log.i("Disabled: " + C.articlesId)
    } else {
      Log.i("Table " + C.articlesId + " already disabled")
    }
  }

  def enable() = {
    if (!admin.isTableEnabled(C.articlesId)) {
      admin.enableTable(C.articlesId)
      Log.i("Enabled: " + C.articlesId)
    } else {
      Log.i("Table " + C.articlesId + " already enabled")
    }
  }

  def add(name: String) = {
    admin.addColumn(C.articlesId, new HColumnDescriptor(name))
  }

  def add(articles: Seq[Article]) = {
    val table = conn.getTable(C.articlesId)
    table.batch(articles.map(put).asJava)
    table.close()
  }

  def add(article: Article) = {
    val table = conn.getTable(C.articlesId)
    table.put(put(article))
    table.close()
  }

  def put(article: Article): Put = {
    val p = new Put(article.id)
    p.addColumn(C.nyFamily, C.id, article.id)
    p.addColumn(C.nyFamily, C.headline, article.hl)
    p.addColumn(C.nyFamily, C.wordCount, article.wc)
    p.addColumn(C.nyFamily, C.date, article.date)
    p.addColumn(C.nyFamily, C.predictions, article.pred)
    p.addColumn(C.nyFamily, C.iptc, article.iptc)
    p.addColumn(C.nyFamily, C.url, article.url)

    p.addColumn(C.googleFamily, C.annotationCount, article.ann.size)
    article.ann.values.foreach(ann => {
      val c = article.id + ":" + ann.index + ":"
      p.addColumn(C.googleFamily, c + C.id, ann.articleId)
      p.addColumn(C.googleFamily, c + C.index, ann.index)
      p.addColumn(C.googleFamily, c + C.phrase, ann.phrase)
      p.addColumn(C.googleFamily, c + C.salience, ann.salience)
      p.addColumn(C.googleFamily, c + C.mentionCount, ann.mc)
      p.addColumn(C.googleFamily, c + C.offset, ann.offset)
      p.addColumn(C.googleFamily, c + C.freeBaseId, ann.fb)
      p.addColumn(C.googleFamily, c + C.tfIdf, ann.tfIdf)
    })
    p
  }

  def toArticle(res: Result): Article = {
    val annotationCount: Int = res.getValue(C.googleFamily, C.annotationCount)
    val annotations = (0 until annotationCount.toInt).map(index => {
      val articleId: String = res.getRow
      val c = articleId + ":" + index + ":"
      Annotation(
        articleId = res.getValue(C.googleFamily, c + C.id),
        index = res.getValue(C.googleFamily, c + C.index),
        phrase = res.getValue(C.googleFamily, c + C.phrase),
        salience = res.getValue(C.googleFamily, c + C.salience),
        mc = res.getValue(C.googleFamily, c + C.mentionCount),
        offset = res.getValue(C.googleFamily, c + C.offset),
        fb = res.getValue(C.googleFamily, c + C.freeBaseId),
        wd = res.getValue(C.googleFamily, c + C.wikidataId),
        tfIdf = res.getValue(C.googleFamily, c + C.tfIdf)
      )
    }).map(a => (a.fb, a)).toMap

    val article = Article(
      id = res.getRow,
      hl = res.getValue(C.nyFamily, C.headline),
      wc = res.getValue(C.nyFamily, C.wordCount),
      date = res.getValue(C.nyFamily, C.date),
      iptc = res.getValue(C.nyFamily, C.iptc),
      url = res.getValue(C.nyFamily, C.url),
      pred = res.getValue(C.nyFamily, C.predictions),
      ann = annotations
    )

    article
  }
}