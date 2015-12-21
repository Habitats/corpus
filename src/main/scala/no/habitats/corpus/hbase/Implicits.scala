package no.habitats.corpus.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes

object Implicits {

  implicit def stringToTableName(str: String): TableName = TableName.valueOf(str)

  // string
  implicit def byteToString(bytes: Array[Byte]): String = Bytes.toString(bytes)

  implicit def stringToByte(str: String): Array[Byte] = Bytes.toBytes(str)

  // seq
  implicit def byteToSeq(bytes: Array[Byte]): Seq[String] = Bytes.toString(bytes).split(C.delim)

  implicit def seqToByte(seq: Seq[String]): Array[Byte] = Bytes.toBytes(seq.mkString(C.delim))

  // set
  implicit def byteToSet(bytes: Array[Byte]): Set[String] = Bytes.toString(bytes).split(C.delim).toSet

  implicit def setToByte(set: Set[String]): Array[Byte] = Bytes.toBytes(set.mkString(C.delim))

  // map
  implicit def byteToMap(bytes: Array[Byte]): Map[String, Int] = Bytes.toString(bytes).split(C.delim).map(i => (i.split(C.delim2)(0), i.split(C.delim2)(1).toInt)).toMap

  implicit def mapToByte(seq: Map[_, _]): Array[Byte] = Bytes.toBytes(seq.map(i => i._1 + C.delim2 + i._2).mkString(C.delim))

  // option
  implicit def byteToOpt(bytes: Array[Byte]): Option[String] = Some(Bytes.toString(bytes))

  implicit def optToByte(opt: Option[String]): Array[Byte] = Bytes.toBytes(opt.getOrElse(""))

  // number convertion
  implicit def byteToInt(bytes: Array[Byte]): Int = Bytes.toInt(bytes)

  implicit def intToByte(n: Int): Array[Byte] = Bytes.toBytes(n)

  implicit def byteToDouble(bytes: Array[Byte]): Double = Bytes.toDouble(bytes)

  implicit def doubleToByte(n: Double): Array[Byte] = Bytes.toBytes(n)
}
