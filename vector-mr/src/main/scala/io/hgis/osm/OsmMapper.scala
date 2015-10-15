package io.hgis.osm

import javax.xml.bind.DatatypeConverter

import io.hgis.osmdomain.WayDAO
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.util.Random

/**
 * Writes the common OSM fields
 *
 * Created by tempehu on 09-Apr-15.
 */
trait OsmMapper {

  val hKey = new ImmutableBytesWritable
  val CFV = "cfv".getBytes
  val CFT = "cft".getBytes

  /**
   * Turns a  postgres hstore string into a Map
   * "building"=>"apartments", "addr:city"=>"Lisbonne"
   *
   * @param tags the string representation of an hstore in postgres
   * @return
   */
  def getTags(tags: String) = {
    tags.split(",")
      .map(_.replace("\"", "").replace(" ", ""))
      .map(_.split("=>"))
      .filter(f => f.size == 2 &&  f(0) != null && f(1) != null)
      .map(f => f(0) -> f(1))
      .toMap
  }

  val metaColMap = Map(
    0 -> "id",
    1 -> "version",
    2 -> "userid",
    3 -> "tstamp",
    4 -> "changesetid",
    5 -> "tags",
    6 -> "default",
    7 -> "hex",
    8 -> "updated"
  )

  /*
   * Zips up tags with their list index, then gets the tag name from the index and maps that to the value
   */
  def getMetaTags(meta: Iterable[String]): Map[String, String] = {
    meta.zipWithIndex.map(m => metaColMap.getOrElse(m._2, "default") -> m._1).toMap
  }

  /**
   * Hex to bytes
   *
   * @param hex the postgis representation of a geometry in hex
   * @return
   */
  def hexToBytes(hex: String): Array[Byte] = DatatypeConverter.parseHexBinary(hex)


  def writeTags(context: Mapper[LongWritable, Text, ImmutableBytesWritable, KeyValue]#Context, tagMap: Map[String, String]): Unit = {

    tagMap.foreach(f => context.write(hKey, new KeyValue(hKey.get(), CFT, f._1.getBytes, Bytes.toBytes(f._2))))

  }
}
