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
import io.hgis.scanutil.TableMapReduceUtilFix
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper, TableReducer}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._
import scala.util.Random

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object CountryProtectionGroupMR {

  val wkbImportOp = OperatorImportFromWkb.local()
  val geom = geomColumn("cfv", "geom") _
  val siteId = stringColumn("cfv", "site_id") _
  val eeId = stringColumn("cfv", "ee_id") _
  val sr = SpatialReference.create(4326)
  val wkbExport = OperatorExportToWkb.local
  val EEZ: String = "E"
  val CFV: Array[Byte] = "cfv".getBytes

  def stringColumn(cf: String, col: String)(v: Result): String = {
    Bytes.toString(v.getValue(cf.getBytes, col.getBytes))
  }

  def intColumn(cf: String, col: String)(v: Result): Int = {
    Bytes.toInt(v.getValue(cf.getBytes, col.getBytes))
  }

  def geomColumn(cf: String, col: String)(v: Result): Geometry = {
    wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(v.getValue(cf.getBytes, col.getBytes)), null)
  }

  def main(args: Array[String]) {

    val conf: Configuration = new Configuration
    conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-02,hadoop-m1")
    conf.set("hbase.zookeeper.clientPort", "2181")
    conf.set("hbase.master", "hadoop-m1")

    val scans = List(
      getScanForTable("country_protection")
    )

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    //Map the country protection
    TableMapReduceUtilFix
      .initTableMapperJob(scans,
        classOf[CountryProtectionMapper],
        classOf[ImmutableBytesWritable],
        classOf[Result],
        job, addDependencyJarsB = true, initCredentials = false)
    TableMapReduceUtil.addDependencyJars(job)

    //Reduces
    TableMapReduceUtil.initTableReducerJob("country_protection_g", classOf[SiteGridReducer], job)

    job.waitForCompletion(true)

  }

  def getScanForTable(tableName: String): Scan = {
    val scan: Scan = new Scan
    scan.addFamily(CFV)
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getBytes)
    scan
  }

  /**
   * Simply maps a site to a grid id
   * Essentially grouping by grid_id - therefore a reducer should receive all the sites (PAs) and manage the overlap
   */
  class CountryProtectionMapper extends TableMapper[ImmutableBytesWritable, Result] {

    val wktImportOp = OperatorImportFromWkt.local()
    val emittedKey = new ImmutableBytesWritable()
    val wkbReader = new WKBReader

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Result]#Context): Unit = {

      //Don't even bother getting the int from bytes - just map the record directly
//      val countryIdCol = result.getValue(CFV, Bytes.toBytes("country_id"))
      val iucnCats = result.getValue(CFV, Bytes.toBytes("iucn_cats"))

      //Emit either a country (not designated) or a designated area
//      emittedKey.set(countryIdCol ++ iucnCats)
      emittedKey.set(iucnCats)
      context.write(emittedKey, result)
      
//      val keys: List[(String, Array[Byte])] = List(("siteId", countryIdCol), ("iucnCats", iucnCats))
    }
  }

  /**
   * Things to watch out for:
   *
   * Empty geometries - esri won't read them
   *
   */
  class SiteGridReducer extends TableReducer[ImmutableBytesWritable, Result, ImmutableBytesWritable] {

    val CF = "cfv".getBytes

    override def reduce(key: ImmutableBytesWritable, values: Iterable[Result],
                        context: Reducer[ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation]#Context): Unit = {


//      val geoms = new SimpleGeometryCursor(values.map(f => geom(f)).toList)
//      val localOp = OperatorUnion.local()
//      val outGeom = localOp.execute(geoms, sr, null).next()
//
      val inKey = key.get()
//      val countryIdCol = Bytes.toInt(inKey.slice(0, 4))
//      val iucnCats = Bytes.toString(inKey.slice(4, inKey.length))
//
      val nextBytes = new Array[Byte](4)
      Random.nextBytes(nextBytes)
      val put = new Put(nextBytes ++ inKey)

//      put.add(CF, "country_id".getBytes, Bytes.toBytes(countryIdCol))
//      put.add(CF, "iucn_cats".getBytes, Bytes.toBytes(iucnCats))
//      val wkb = wkbExport.execute(WkbExportFlags.wkbExportDefaults, outGeom, null).array()
//      put.add(CF, "geom".getBytes, wkb)
//      context.write(null, put)

      val iucnCats = Bytes.toString(inKey)
      put.add(CF, "iucn_cats".getBytes, Bytes.toBytes(iucnCats))
      put.add(CF, "count".getBytes, Bytes.toBytes(values.size))
      context.write(null, put)

    }

  }

}

