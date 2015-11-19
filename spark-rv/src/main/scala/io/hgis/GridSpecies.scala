package io.hgis

import com.esri.core.geometry.{Geometry, WktImportFlags, OperatorImportFromWkt, OperatorImportFromWkb}
import io.hgis.accessutil.AccessUtil
import io.hgis.hdomain.{GriddedEntity, GriddedObjectDAO}
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteDAO
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
 *
 * Created by tempehu on 23-Oct-14.
 */
object GridSpecies {

  val wkbImportOp = OperatorImportFromWkb.local()


//  def geomColumn(cf: String, col: String)(v: Result): Geometry = {
//    wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(v.getValue(cf.getBytes, col.getBytes)), null)
//  }

  val geomColumn  = AccessUtil.stringColumn("cfv", "geom") _

  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val tableName = "site"
    println("Table " + tableName)

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMNS, "cfv:geom")


    val sparkConf = new SparkConf().setAppName("pa_grid_spark")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    println(rdd.count())

    val hTable = new HTable(conf, "pa_grid_spark")

    def doIX = doIntersection(hTable) _

    rdd.foreach(f => doIX(f._2))

  }

  def doIntersection(htable: HTable)(result: Result) {

      val wktImportOp = OperatorImportFromWkt.local()

//    override def map(key: ImmutableBytesWritable, result: Result,
//                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      val site = SiteDAO.fromResult(result)

      //Get the grids and id lists
      val gridGeoms = site.gridCells.map(f => wktImportOp.execute(WktImportFlags.wktImportDefaults, Geometry.Type.Polygon, f, null))
      val siteGrids = IntersectUtil.executeIntersect(site, gridGeoms, site.gridIdList.map(_.toInt), 4)

      for (sg: GriddedEntity <- siteGrids) {

        val put = GriddedObjectDAO.toPut(sg, GriddedObjectDAO.getRowKey(site.entityId.toInt, sg.gridId))

        put.add(GriddedObjectDAO.getCF, "cat_id".getBytes, Bytes.toBytes(site.catId))
        put.add(GriddedObjectDAO.getCF, "is_designated".getBytes, Bytes.toBytes(site.isDesignated))
        put.add(GriddedObjectDAO.getCF, "is_point".getBytes, Bytes.toBytes(site.isPoint))

        htable.put(put)
//      }
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

