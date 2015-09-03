package io.hgis.spark

import com.esri.core.geometry.OperatorImportFromWkb
import com.vividsolutions.jts.io.{WKTReader, WKBReader}
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Created by tempehu on 23-Oct-14.
 */
object DebugGeom {

  val wkbImportOp = OperatorImportFromWkb.local()


//  def geomColumn(cf: String, col: String)(v: Result): Geometry = {
//    wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(v.getValue(cf.getBytes, col.getBytes)), null)
//  }

  val geomColumn  = AccessUtil.geomColumn(new WKBReader(), "cfv") _
  val wktColumn   = AccessUtil.wktColumn(new WKTReader(), "cfv") _

  def debugGeom(wktR: WKTReader, v: Result): String = {

//    val id = Bytes.toInt(v.vgetValue("cfv".getBytes, "id".getBytes))

    val s: String = Bytes.toString(v.getValue("cfv".getBytes, "wkt".getBytes))
    try {
      val geom = wktR.read(s)
      "" //OK
    } catch {
      case _: Throwable => s
    }

  }

  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

//    siteGrid.geom = wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(result.getValue(getCF, GEOM)), null)
    val tableName = "ways"
    println("Table " + tableName)

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMNS, "cfv:linestring")
    conf.set(TableInputFormat.SCAN, getScan)

    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[8]")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])


//    val x = rdd.map(f => debugGeom(new WKTReader(), f._2))
//    x.filter(s => !s.equals("")).foreach(println)
    val strs = rdd.map(f => Bytes.toString(f._2.getValue("cfv".getBytes, "wkt".getBytes)))//.foreach(println)

    strs.foreach(println)
//    val wktR = new WKTReader()

//    for (s <- strs ) {
//      try {
//        val geom = wktR.read(s)
//      } catch {
//        case _: Throwable => println(s)
//      }
//    }
//    rdd.map(f => wktColumn(f._2)).foreach(f => println(f.getNumPoints))




//    val x = rdd.map(f => stringColumn("cfv", "iucn_cats")(f._2)).countByValue()

//    val y: Map[Int, Long] = rdd.map(f => intColumn("cfv", "nc")(f._2)).countByValue()


//    println("====NC====")
//    y.foreach(f => println(f._1 + "->" + f._2))

//    val x = rdd.map(f => intColumn("cfv", "ns")(f._2)).countByValue()
//      .filter(f => isDesignated(f._2))
//      .map(f => SiteGridDAO.fromResult(f._2)).count()
//        val x = rdd
//          .map(f => gridId(f._2)).count()


//        nSites(rdd)
//        isDesignatedCheck(rdd)
//    categoryCheck(rdd)
  }

  def encodeScan(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getScan: String = {

    val s1 = new Scan()
    s1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "ways".getBytes)
    s1.addFamily("cfv".getBytes)
    s1.addColumn("cfv".getBytes, "wkt".getBytes)
    s1.addColumn("cfv".getBytes, "id".getBytes)
    s1.setMaxResultsPerColumnFamily(100)

//    val s2 = new Scan()
//    s2.addColumn("cfv".getBytes, "site_id".getBytes)
//    s2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "pa_grid".getBytes())

    encodeScan(s1)// + "," + encodeScan(s2)
  }


//  def categoryCheck(rdd: RDD[(ImmutableBytesWritable, Result)]) = {
//    val x = rdd.map(f => iucnCat(f._2)).countByValue()
//    x.foreach(f => println(f._1 + "->" + f._2))
//  }
//
//  def designatedCheck(rdd: RDD[(ImmutableBytesWritable, Result)]) = {
//    val x = rdd.map(f => isDesignated(f._2)).countByValue()
//    x.foreach(f => println(f._1 + "->" + f._2))
//  }
//
//  def nSites(rdd: RDD[(ImmutableBytesWritable, Result)]): Unit = {
//    val x = rdd.map(f => Bytes.toInt(f._2.getValue("cfv".getBytes, "site_id".getBytes))).distinct().count()
//    println("N site ids: " + x)
//  }

}

