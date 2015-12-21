package no.habitats.corpus.hbase

import C._
import no.habitats.corpus.hbase.Implicits._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter, SingleColumnValueFilter}

object F {
  def singleColumnEquals(column: String, filter: String): Filter = new SingleColumnValueFilter(nyFamily, column, CompareOp.EQUAL, filter)
}
