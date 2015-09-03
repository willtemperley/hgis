package io.hgis.accessutil

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKTReader, WKBReader}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

/**
 * Some functions to make accessing hbase columns simpler
 *
 * Created by willtemperley@gmail.com on 12-Dec-14.
 */
object AccessUtil {

  /**
    */
  def booleanColumn(cf: String, col: String)(v: Result): Boolean = {
    Bytes.toBoolean(v.getValue(cf.getBytes, col.getBytes))
  }

  def stringColumn(cf: String, col: String)(v: Result): String = {
    Bytes.toString(v.getValue(cf.getBytes, col.getBytes))
  }

  def intColumn(cf: String, col: String)(v: Result): Int = {
    Bytes.toInt(v.getValue(cf.getBytes, col.getBytes))
  }

  def geomColumn(wkbReader: WKBReader, cf: String, col: String = "geom")(v: Result): Geometry = {
    val bytes: Array[Byte] = v.getValue(cf.getBytes, col.getBytes)
    if (bytes == null) null else wkbReader.read(bytes)
  }

  def wktColumn(wktReader: WKTReader, cf: String, col: String = "wkt")(v: Result): Geometry = {
    wktReader.read(Bytes.toString(v.getValue(cf.getBytes, col.getBytes)))
  }
}
