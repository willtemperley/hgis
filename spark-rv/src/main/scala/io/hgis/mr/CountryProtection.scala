package io.hgis.mr

import java.nio.ByteBuffer

import com.esri.core.geometry.{Geometry, OperatorImportFromWkb}
import io.hgis.ConfigurationFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{MultiTableInputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
 *
 * Created by tempehu on 23-Oct-14.
 */
object CountryProtection {

  val wkbImportOp = OperatorImportFromWkb.local()
  /**
   */
  def booleanColumn(cf: String, col: String)(v: Result): Boolean = {
    Bytes.toBoolean(v.getValue(cf.getBytes, col.getBytes))
  }

  val isDesignated: (Result) => Boolean = booleanColumn("cfv", "is_designated")

  def stringColumn(cf: String, col: String)(v: Result): String = {
    Bytes.toString(v.getValue(cf.getBytes, col.getBytes))
  }

  val iucnCat: (Result) => String = stringColumn("cfv", "iucn_cat")

  def intColumn(cf: String, col: String)(v: Result): Int = {
    val bytes = v.getValue(cf.getBytes, col.getBytes)
    if (bytes != null) Bytes.toInt(bytes) else 0
  }

  val siteId: (Result) => Int = intColumn("cfv", "site_id")

  val gridId: (Result) => Int = intColumn("cfv", "grid_id")

  def geomColumn(cf: String, col: String)(v: Result): Geometry = {
    wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(v.getValue(cf.getBytes, col.getBytes)), null)
  }

  val geomColumn: (Result) => String = stringColumn("cfv", "geom")

  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val tableName = "country_protection"
    println("Table " + tableName)

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cfv")
//    conf.set(TableInputFormat.SCAN_COLUMNS, "cfv:site_id cfv:is_designated cfv:iucn_cat cfv:geom cfv:grid_id cfv:ns cfv:nc")
    conf.set(MultiTableInputFormat.SCANS, getScans())

    val sparkConf = new SparkConf().setAppName("site_grid").setMaster("local[8]")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

//    val shapes = rdd.map(f => SiteGridDAO.fromResult(f._2))

    val x: Map[Int, Long] = rdd.map(f => intColumn("cfv", "ns")(f._2)).countByValue()

    val y: Map[Int, Long] = rdd.map(f => intColumn("cfv", "nc")(f._2)).countByValue()

    println("====NS====")
    x.foreach(f => println(f._1 + "->" + f._2))

    println("====NC====")
    y.foreach(f => println(f._1 + "->" + f._2))

//    val x = rdd.map(f => intColumn("cfv", "ns")(f._2)).countByValue()
//      .filter(f => isDesignated(f._2))
//      .map(f => SiteGridDAO.fromResult(f._2)).count()
//        val x = rdd
//          .map(f => gridId(f._2)).count()


//        nSites(rdd)
//        isDesignatedCheck(rdd)
//    categoryCheck(rdd)
  }


  def getScans(): String = {
    val s1 = new Scan()
    s1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "pa_grid".getBytes())
    s1.addFamily("cfv".getBytes)
    s1.addColumn("cfv".getBytes, "site_id".getBytes)
    s1.addColumn("cfv".getBytes, "geom".getBytes)

//    val s2 = new Scan()
//    s2.addColumn("cfv".getBytes, "site_id".getBytes)
//    s2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "pa_grid".getBytes())

    encodeScan(s1)// + "," + encodeScan(s2)
  }

  def encodeScan(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def categoryCheck(rdd: RDD[(ImmutableBytesWritable, Result)]) = {
    val x = rdd.map(f => iucnCat(f._2)).countByValue()
    x.foreach(f => println(f._1 + "->" + f._2))
  }

  def designatedCheck(rdd: RDD[(ImmutableBytesWritable, Result)]) = {
    val x = rdd.map(f => isDesignated(f._2)).countByValue()
    x.foreach(f => println(f._1 + "->" + f._2))
  }

  def nSites(rdd: RDD[(ImmutableBytesWritable, Result)]): Unit = {
    val x = rdd.map(f => Bytes.toInt(f._2.getValue("cfv".getBytes, "site_id".getBytes))).distinct().count()
    println("N site ids: " + x)
  }

}

