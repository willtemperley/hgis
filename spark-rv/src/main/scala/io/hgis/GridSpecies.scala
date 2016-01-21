package io.hgis

import java.io.{DataInputStream, ByteArrayInputStream, DataOutputStream}

import com.esri.core.geometry.{Geometry, WktImportFlags, OperatorImportFromWkt, OperatorImportFromWkb}
import com.sun.corba.se.spi.ior.Writeable
import io.hgis.accessutil.AccessUtil
import io.hgis.hdomain.{GriddedEntity, GriddedObjectDAO}
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteDAO
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapper, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


import com.esri.core.geometry.{Geometry, OperatorImportFromWkt, WktImportFlags}
import io.hgis.hdomain.{GriddedEntity, GriddedObjectDAO}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, Mapper}
/**
  * spark-submit --class io.hgis.GridSpecies --jars target/dependency/hbase-mr-0.1-SNAPSHOT.jar,target/dependency/vector-mr-0.1-SNAPSHOT.jar,target/dependency/esri-geometry-api-1.2.jar --master yarn-client target/spark-rv-0.1-SNAPSHOT.jar
 *
 * Created by tempehu on 23-Oct-14.
 */
object GridSpecies {

  val wkbImportOp = OperatorImportFromWkb.local()


//  def geomColumn(cf: String, col: String)(v: Result): Geometry = {
//    wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(v.getValue(cf.getBytes, col.getBytes)), null)
//  }

  val geomColumn  = AccessUtil.stringColumn("cfv", "geom") _

  def confToBytes(configuration: Configuration): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    configuration.write(new DataOutputStream(baos))
    baos.toByteArray
  }

  def confFromBytes(byteArray: Array[Byte]) = {
    val bais = new ByteArrayInputStream(byteArray)
    val nconf = new Configuration()
    nconf.readFields(new DataInputStream(bais))
    nconf
  }

  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val tableName = "site_test"
    println("Table " + tableName)

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cfv")


    val sparkConf = new SparkConf().setAppName("pa_grid_spark")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    println(rdd.count())


//    def doIX = doIntersection(conf) _

//    rdd.foreach(f => doIX(f._2))
    val confBytes = confToBytes(conf)

    rdd.foreachPartition(partition => partition.foreach(f => doIntersection(confBytes, f._2)))

  }

  def doIntersection(conf: Array[Byte], result: Result) {

      val hTable = new HTable(confFromBytes(conf), "pa_grid_spark")

      val wktImportOp = OperatorImportFromWkt.local()

      val site = SiteDAO.fromResult(result)

      //Get the grids and id lists
      val gridGeoms = site.gridCells.map(f => wktImportOp.execute(WktImportFlags.wktImportDefaults, Geometry.Type.Polygon, f, null))
      val siteGrids = IntersectUtil.executeIntersect(site, gridGeoms, site.gridIdList.map(_.toInt), 4)

      for (sg: GriddedEntity <- siteGrids) {

        val put = GriddedObjectDAO.toPut(sg, GriddedObjectDAO.getRowKey(site.entityId.toInt, sg.gridId))

        put.add(GriddedObjectDAO.getCF, "cat_id".getBytes, Bytes.toBytes(site.catId))
        put.add(GriddedObjectDAO.getCF, "is_designated".getBytes, Bytes.toBytes(site.isDesignated))
        put.add(GriddedObjectDAO.getCF, "is_point".getBytes, Bytes.toBytes(site.isPoint))

        hTable.put(put)
    }

  }

  def getScans: String = {
    val s1 = new Scan()
    s1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "pa_grid".getBytes)
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

}

