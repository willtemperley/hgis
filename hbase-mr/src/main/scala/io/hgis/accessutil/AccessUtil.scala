package io.hgis.accessutil

import java.io.{DataInputStream, ByteArrayInputStream, DataOutputStream}

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKTReader, WKBReader}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.WritableUtils

/**
 * Some functions to make accessing hbase columns simpler
 *
 * Created by willtemperley@gmail.com on 12-Dec-14.
 */
object AccessUtil {

  def transferCell(result: Result, put: Put, cf: Array[Byte])(col: Array[Byte]): Unit = {
    put.add(cf, col, result.getValue(cf, col))
  }


  def serializeStringArray(strings: Array[String]): Array[Byte] = {
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val dOut = new DataOutputStream(byteArrayOutputStream)
    WritableUtils.writeStringArray(dOut, strings.toArray)
    val encoded = byteArrayOutputStream.toByteArray
    encoded
  }

  def deserializeStringArray(bytes: Array[Byte]): Array[String] = {
    val bis = new ByteArrayInputStream(bytes)
    val inputStream: DataInputStream = new DataInputStream(bis)
    val strings = WritableUtils.readStringArray(inputStream)
    strings
  }

  /**
    */
  def booleanColumn(cf: String, col: String)(v: Result): Boolean = {
    Bytes.toBoolean(v.getValue(cf.getBytes, col.getBytes))
  }

  def stringColumn(cf: String, col: String)(v: Result): String = stringColumn(cf.getBytes, col)(v: Result)

  def stringColumn(cf: Array[Byte], col: String)(v: Result): String = {
    val bytes = v.getValue(cf, col.getBytes)
    if (bytes == null) null else Bytes.toString(bytes)
  }

  def intColumn(cf: String, col: String)(v: Result): Int = intColumn(cf.getBytes, col)(v: Result)

  def intColumn(cf: Array[Byte], col: String)(v: Result): Int = {
    Bytes.toInt(v.getValue(cf, col.getBytes))
  }

  def longColumn(cf: String, col: String)(v: Result): Long = longColumn(cf.getBytes, col)(v: Result)

  def longColumn(cf: Array[Byte], col: String)(v: Result): Long = {
    Bytes.toLong(v.getValue(cf, col.getBytes))
  }

  def geomColumn(wkbReader: WKBReader, cf: String, col: String = "geom")(v: Result): Geometry = geomColumn(wkbReader, cf.getBytes, col)(v:Result)

  def geomColumn(wkbReader: WKBReader, cf: Array[Byte], col: String)(v: Result): Geometry = {
    val bytes: Array[Byte] = v.getValue(cf, col.getBytes)
    if (bytes == null) null else wkbReader.read(bytes)
  }

  def wktColumn(wktReader: WKTReader, cf: String, col: String = "wkt")(v: Result): Geometry = {
    wktReader.read(Bytes.toString(v.getValue(cf.getBytes, col.getBytes)))
  }
}
