package io.hgis.vector.mr

/**
 * Intersects PAs and their grids
 *
 * Copies all output to another HBase table
 *
 * Required some hacking to work:
 * https://github.com/ndimiduk/hbase-fatjar
 *
 * Magic incantation:
 * HADOOP_CLASSPATH=$(hbase classpath) hadoop jar target/hbase-mr-0.1-SNAPSHOT-jar-with-dependencies.jar io.hgis.mr.SiteOverlapMR
 *
 * Created by willtemperley@gmail.com on 17-Nov-14.
 */

import java.lang.Iterable
import java.nio.ByteBuffer

import com.esri.core.geometry._
import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.scanutil.TableMapReduceUtilFix
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper, TableReducer}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.util.Random

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object EcoregionEEZSiteGroupMR {

  val wkbImportOp = OperatorImportFromWkb.local()

  val geom = geomColumn("cfv", "geom") _

  val siteId = AccessUtil.stringColumn("cfv", "site_id") _

  val eeId = AccessUtil.stringColumn("cfv", "ee_id") _

  val sr = SpatialReference.create(4326)

  val wkbExport = OperatorExportToWkb.local

  val EEZ: String = "E"

  val CF = "cfv".getBytes

  def geomColumn(cf: String, col: String)(v: Result): Geometry = {
    wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(v.getValue(cf.getBytes, col.getBytes)), null)
  }

  def main(args: Array[String]) {

    val conf: Configuration = new Configuration
    conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-02,hadoop-m1")
    conf.set("hbase.zookeeper.clientPort", "2181")
    conf.set("hbase.master", "hadoop-m1")

    val scans = List(
      getScanForTable("site_ecoregion_eez")
    )

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    //Maps the site grid
    TableMapReduceUtilFix
      .initTableMapperJob(scans,
        classOf[SiteGridMapper],
        classOf[ImmutableBytesWritable],
        classOf[Result],
        job, addDependencyJarsB = true, initCredentials = false)
    TableMapReduceUtil.addDependencyJars(job)

    //Reduces
    TableMapReduceUtil.initTableReducerJob("site_ecoregion_eez_g", classOf[SiteGridReducer], job)

    job.waitForCompletion(true)

  }

  def getScanForTable(tableName: String): Scan = {
    val scan: Scan = new Scan
    scan.addFamily(CF)
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getBytes)
    scan
  }

  /**
   * Simply maps a site to a grid id
   * Essentially grouping by grid_id - therefore a reducer should receive all the sites (PAs) and manage the overlap
   */
  class SiteGridMapper extends TableMapper[ImmutableBytesWritable, Result] {

    val wktImportOp = OperatorImportFromWkt.local()
    val emittedKey = new ImmutableBytesWritable()
    val wkbReader = new WKBReader

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Result]#Context): Unit = {

      //Don't even bother getting the int from bytes - just map the record directly
      val siteId = result.getValue(CF, Bytes.toBytes("site_id"))
      val eeId = result.getValue(CF, Bytes.toBytes("ee_id"))

      val hasArea = wkbReader.read(result.getValue(CF, "geom".getBytes)).getArea > 0

      //Emit either a country (not designated) or a designated area
      if (hasArea) {
        emittedKey.set(siteId ++ eeId)
        context.write(emittedKey, result)
      }
    }
  }

  /**
   * Things to watch out for:
   *
   * Empty geometries - esri won't read them
   *
   */
  class SiteGridReducer extends TableReducer[ImmutableBytesWritable, Result, ImmutableBytesWritable] {


    override def reduce(key: ImmutableBytesWritable, values: Iterable[Result],
                        context: Reducer[ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation]#Context): Unit = {



      val inKey = key.get()
      val siteId = Bytes.toInt(inKey.slice(0, 4))
      val eeId = Bytes.toInt(inKey.slice(4, 8))
      val nextBytes = new Array[Byte](4)
      Random.nextBytes(nextBytes)
      val put = new Put(nextBytes ++ inKey)

      put.add(CF, "ee_id".getBytes, Bytes.toBytes(eeId))
      put.add(CF, "site_id".getBytes, Bytes.toBytes(siteId))
      context.write(null, put)

//      val outGeoms: GeometryCursor = localOp.execute(geoms, sr, null)

//      val unioned = Iterator.continually(outGeoms.next).takeWhile(_ != null)
//      for (u <- unioned) {
//        if (u.calculateArea2D() > 0) {
//          //Should only happen once ...
//          val nextBytes = new Array[Byte](4)
//          Random.nextBytes(nextBytes)
//          //a put with a very paranoid key
//          val put = new Put(nextBytes ++ inKey)
//
//          put.add(CF, "ee_id".getBytes, Bytes.toBytes(eeId))
//          put.add(CF, "site_id".getBytes, Bytes.toBytes(siteId))
//          val wkb = wkbExport.execute(WkbExportFlags.wkbExportDefaults, u, null).array()
//          put.add(CF, "geom".getBytes, wkb)
//          context.write(null, put)
//
//        }
//      }
    }

  }

}

