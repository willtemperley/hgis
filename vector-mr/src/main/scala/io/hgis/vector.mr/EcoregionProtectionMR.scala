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

import com.esri.core.geometry._
import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.scanutil.TableMapReduceUtilFix
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper, TableReducer}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._
import scala.util.Random

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object EcoregionProtectionMR {

  val iucnCat = AccessUtil.stringColumn("cfv", "iucn_cat") _
  val catId = AccessUtil.intColumn("cfv", "cat_id") _
  val siteId = AccessUtil.intColumn("cfv", "site_id") _
  val gridId = AccessUtil.intColumn("cfv", "grid_id") _

  val sr = SpatialReference.create(4326)

  val wkbExport = OperatorExportToWkb.local

  val DISCRIMINATOR: String = "E"

  val CAT_ID: String = "CAT_ID"

  def main(args: Array[String]) {

    val conf: Configuration = new Configuration
    conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-02,hadoop-m1")
    conf.set("hbase.zookeeper.clientPort", "2181")
    conf.set("hbase.master", "hadoop-m1")

    if (args.length != 2) {
      println("Please specify whether it's cumulative and the cat id.")
    }

    val cumulative = args(0).toBoolean
    val catId = args(1).toInt
    conf.set(CAT_ID, args(1))

    val scans = List(getProtectedAreaScan("pa_grid", catId, cumulative),
      getScanForTable("ee_grid"))

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
    TableMapReduceUtil.initTableReducerJob("ee_protection", classOf[SiteGridReducer], job)

    job.waitForCompletion(true)

  }

  def getScanForTable(tableName: String): Scan = {
    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getBytes)
    scan
  }

  def getProtectedAreaScan(tableName: String, catId: Int, cumulative: Boolean): Scan = {
    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getBytes)

    val op = if (cumulative) CompareOp.LESS_OR_EQUAL else CompareOp.EQUAL

    val filters =
      List(
        new SingleColumnValueFilter(SiteGridDAO.getCF, "cat_id".getBytes, op, new BinaryComparator(Bytes.toBytes(catId)))
//        new SingleColumnValueFilter(SiteGridDAO.getCF, "is_designated".getBytes, CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(true)))
      )

    val filterList = new FilterList(filters)
    scan.setFilter(filterList)

    scan
  }

  /**
   * Simply maps a site to a grid id
   * Essentially grouping by grid_id - therefore a reducer should receive all the sites (PAs) and manage the overlap
   */
  class SiteGridMapper extends TableMapper[ImmutableBytesWritable, Result] {

    val wkbReader = new WKBReader

    val emittedKey = new ImmutableBytesWritable()

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Result]#Context): Unit = {

      /*
       * Everything is mapped to the grid_id, retrieved and emitted as binary
       */
      val gridId = result.getValue(SiteGridDAO.getCF, SiteGridDAO.GRID_ID)
      emittedKey.set(gridId)

      val hasArea = wkbReader.read(result.getValue(SiteGridDAO.getCF, SiteGridDAO.GEOM)).getArea > 0
      if (hasArea) {
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

    val CF = "cfv".getBytes
    val emittedKey = new ImmutableBytesWritable()

    override def reduce(key: ImmutableBytesWritable, values: Iterable[Result],
                        context: Reducer[ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation]#Context): Unit = {

      val (cGrids, sGrids) =
        values.map(r => SiteGridDAO.fromResult(r))
          .partition(f => f.iucnCat.equals(DISCRIMINATOR))

      val inGeoms = sGrids.map(_.geom).toList
      val diffOp = OperatorDifference.local()

      for (cG <- cGrids) {

        var diff: Geometry = cG.geom
        for (g <- inGeoms) {
          diff = diffOp.execute(diff, g, sr, null)
        }

//        //Calculate protection minus country
//        val d = diffOp.execute(cG.geom, diff, sr, null)

        //Do nothing if there's no area
//        if (diff.calculateArea2D() == 0) return

        val nextBytes = new Array[Byte](4)
        Random.nextBytes(nextBytes)
        val suffix = Bytes.toBytes(cG.gridId) ++ Bytes.toBytes(cG.entityId)
        //a put with a very paranoid key
        val put = new Put(nextBytes ++ suffix)

        //This is quite confusing, because country_id and site_id are both actually ee_id
        put.add(CF, "country_id".getBytes, Bytes.toBytes(cG.entityId))
        put.add(CF, "grid_id".getBytes, Bytes.toBytes(cG.gridId))

        val wkb = wkbExport.execute(WkbExportFlags.wkbExportDefaults, diff, null).array()
        put.add(CF, "geom".getBytes, wkb)

        val catId = context.getConfiguration.get(CAT_ID)
        put.add(CF, "cat_id".getBytes, Bytes.toBytes(catId.toInt))

        emittedKey.set(nextBytes ++ suffix)
        context.write(emittedKey, put)

      }
    }
  }

}

