package io.hgis.load.osm

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.scanutil.MultiScan
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, MultiTableInputFormat}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 *
 * Created by tempehu on 23-Oct-14.
 */
object TagCount {

  val wkt = new WKTReader()
  val wkb = new WKBReader()
  val geomColumn  = AccessUtil.geomColumn(wkb, "cfv", "geom") _
  val idColumn  = AccessUtil.intColumn("cfv", "id") _
  val highwayColumn  = AccessUtil.stringColumn("cft", "highway") _
  val speedColumn  = AccessUtil.stringColumn("cft", "maxspeed") _

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("tagcount").setMaster("local[12]")
    val sc = new SparkContext(sparkConf)

    val conf: Configuration = ConfigurationFactory.get
    conf.set(TableInputFormat.SCAN, getScans)
    conf.set(TableInputFormat.INPUT_TABLE, "transport")

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
     .map(f => idColumn(f._2))

    val ids = rdd.collect()

    ids.foreach(println)

  }

  def ix(geom: Geometry, env: Geometry): Unit = {
    val ixes = geom.getEnvelope.intersects(env.getEnvelope)
  }


  def getScans: String = {
//    val s1 = new Scan()
//    s1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "country".getBytes)
//    s1.addFamily("cfv".getBytes)
//    s1.addColumn("cfv".getBytes, "site_id".getBytes)
//    s1.addColumn("cfv".getBytes, "geom".getBytes)

    val s2 = new Scan()
    s2.addColumn("cfv".getBytes, "id".getBytes)
//    s2.addColumn("cfv".getBytes, "geom".getBytes)
//    s2.addColumn("cft".getBytes, "highway".getBytes)
//    s2.addColumn("cft".getBytes, "maxspeed".getBytes)
//    s2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "transport".getBytes)
    s2.setFilter(new SingleColumnValueFilter("cfv".getBytes, "geom".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null)))
    s2.setFilter(new SingleColumnValueFilter("cft".getBytes, "highway".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null)))
    s2.setFilter(new SingleColumnValueFilter("cft".getBytes, "maxspeed".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null)))

    MultiScan.encodeScan(s2)
//    MultiScan.serializeScans(Array(s1, s2))
  }
}

