package io.hgis.scanutil

/**
 * Created by tempehu on 16-Jan-15.
 */
class KeySerializer(keys: List[(String, Array[Byte])]) {

  val k = keys.map(f => f._1)
  val v = keys.map(f => f._2)
  val l = keys.map(f => f._2.length)
  val groupedKey = v.foldLeft(new Array[Byte](0))(_ ++ _)

}
