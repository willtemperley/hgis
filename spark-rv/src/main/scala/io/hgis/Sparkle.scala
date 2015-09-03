package io.hgis

import java.nio.ByteBuffer

import com.esri.core.geometry.{Geometry, OperatorImportFromWkb}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Created by tempehu on 23-Oct-14.
 */
object Sparkle {

  val wkbImportOp = OperatorImportFromWkb.local()
  /**
   * Get a boolean from a result
   *
   * @param cf
   * @param col
   * @param v
   * @return
   */
  def booleanColumn(cf: String, col: String)(v: Result): Boolean = {
    Bytes.toBoolean(v.getValue(cf.getBytes, col.getBytes))
  }

  val isDesignated: (Result) => Boolean = booleanColumn("cfv", "is_designated")

  def stringColumn(cf: String, col: String)(v: Result): String = {
    Bytes.toString(v.getValue(cf.getBytes, col.getBytes))
  }

  val iucnCat: (Result) => String = stringColumn("cfv", "iucn_cat")

  val catId: (Result) => Int = intColumn("cfv", "cat_id")

  def intColumn(cf: String, col: String)(v: Result): Int = {

    val value = v.getValue(cf.getBytes, col.getBytes)
    if(value != null) Bytes.toInt(value) else 0
  }

  val siteId: (Result) => Int = intColumn("cfv", "site_id")

  def geomColumn(cf: String, col: String)(v: Result): Geometry = {
    wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(v.getValue(cf.getBytes, col.getBytes)), null)
  }

  val geomColumn: (Result) => String = stringColumn("cfv", "geom")

  def main(args: Array[String]) {

    val conf: Configuration = new Configuration
    conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-02,hadoop-m1")
    conf.set("hbase.zookeeper.clientPort", "2181")
    conf.set("hbase.master", "hadoop-m1")

//    siteGrid.geom = wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(result.getValue(getCF, GEOM)), null)
//    val tableName = "pa_grid"
//    println("Table " + tableName)

//    conf.set(TableInputFormat.INPUT_TABLE, tableName)
//    conf.set(TableInputFormat.SCAN_COLUMNS, "cfv:site_id cfv:is_designated cfv:iucn_cat")
    conf.set(MultiTableInputFormat.SCANS, getScans)

    val sparkConf = new SparkConf().setAppName("pa_grid").setMaster("local[8]")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[MultiTableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

//    val shapes = rdd.map(f => SiteGridDAO.fromResult(f._2))
//    println(shapes.count())
    //    println(shapes.count())

//    val x = rdd
//      .filter(f => isDesignated(f._2))
//      .map(f => geomColumn(f._2)).count()
//
//    println("N : " + x)

//        nSites(rdd)
//        isDesignatedCheck(rdd)
    categoryCheck(rdd)
    println("================================================")
    catIdCheck(rdd)
  }


  def getScans: String = {
    val s1 = new Scan()
    s1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "pa_grid".getBytes())
    s1.addFamily("cfv".getBytes)
    s1.addColumn("cfv".getBytes, "iucn_cat".getBytes)
    s1.addColumn("cfv".getBytes, "cat_id".getBytes)
//    s1.addColumn("cfv".getBytes, "site_id".getBytes)
//    s1.addColumn("cfv".getBytes, "geom".getBytes)

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

  def catIdCheck(rdd: RDD[(ImmutableBytesWritable, Result)]) = {
    val x = rdd.map(f => catId(f._2)).countByValue()
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

