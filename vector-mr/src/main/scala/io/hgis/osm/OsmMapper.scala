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

  /**
   * Hex to bytes
   *
   * @param hex the postgis representation of a geometry in hex
   * @return
   */
  def hexToBytes(hex: String): Array[Byte] = DatatypeConverter.parseHexBinary(hex)


  /**
   * Writes out the common OSM fields and sets the ID
   *
   * @param context the contex to write to
   * @param fields  the f
   */
  def writeMetaData(context: Mapper[LongWritable, Text, ImmutableBytesWritable, KeyValue]#Context, fields: Array[String]): Unit = {

    val id = fields(0).toLong

    val nextBytes = new Array[Byte](4)
    Random.nextBytes(nextBytes)
    hKey.set(nextBytes ++ Bytes.toBytes(id))

    {
      val kv = new KeyValue(hKey.get(), CFV, WayDAO.ID, Bytes.toBytes(id))
      context.write(hKey, kv)
    }
    {
      val version = fields(1).toInt
      val kv = new KeyValue(hKey.get(), CFV, WayDAO.VERSION, Bytes.toBytes(version))
      context.write(hKey, kv)
    }
    {
      val userId = fields(2).toInt
      val kv = new KeyValue(hKey.get(), CFV, WayDAO.USER_ID, Bytes.toBytes(userId))
      context.write(hKey, kv)
    }
    {
      val tStamp = fields(3)
      val kv = new KeyValue(hKey.get(), CFV, WayDAO.TSTAMP, Bytes.toBytes(tStamp))
      context.write(hKey, kv)
    }
    {
      val changesetId = fields(4).toLong
      val kv = new KeyValue(hKey.get(), CFV, WayDAO.CHANGESET_ID, Bytes.toBytes(changesetId))
      context.write(hKey, kv)
    }
    {
      val updated = fields(8)
      val kv = new KeyValue(hKey.get(), CFV, WayDAO.DATE_UPDATED, Bytes.toBytes(updated))
      context.write(hKey, kv)
    }
  }

  def writeTags(context: Mapper[LongWritable, Text, ImmutableBytesWritable, KeyValue]#Context, tagMap: Map[String, String]): Unit = {
    if (!tagMap.isDefinedAt("highway")) {
      throw new RuntimeException("HAS TO BE A HIGHWAY")
    }


    tagMap.foreach(f => context.write(hKey, new KeyValue(hKey.get(), CFT, f._1.getBytes, Bytes.toBytes(f._2))))
  }
}
