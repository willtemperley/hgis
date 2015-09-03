package io.hgis.vector.mr

/**
 * Intersects PAs and Ecoregions
 *
 * intersects PAs and Ecoregion EEZs (pa_grid and ee_grid tables) and populates site_ecoregion_eez
 *
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
import io.hgis.scanutil.TableMapReduceUtilFix
import io.hgis.vector.domain.{TSiteGrid, SiteGridDAO}
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
object EcoregionEEZSiteMR {

  def stringColumn(cf: String, col: String)(v: Result): String = {
    Bytes.toString(v.getValue(cf.getBytes, col.getBytes))
  }

  val iucnCat: (Result) => String = stringColumn("cfv", "iucn_cat")

  val sr = SpatialReference.create(4326)

  val wkbExport = OperatorExportToWkb.local

  val EEZ: String = "E"

  def main(args: Array[String]) {

    val conf: Configuration = new Configuration
    conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-02,hadoop-m1")
    conf.set("hbase.zookeeper.clientPort", "2181")
    conf.set("hbase.master", "hadoop-m1")

    val scans = List(
      getScanForTable("pa_grid"),
      getScanForTable("ee_grid")
    )

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    //Maps the site grid
    TableMapReduceUtilFix
      .initTableMapperJob(scans,
        classOf[SiteGridMapper],
        classOf[ImmutableBytesWritable],
        classOf[Result],
        job, true, false)
    TableMapReduceUtil.addDependencyJars(job)

    //Reduces
    TableMapReduceUtil.initTableReducerJob("site_ecoregion_eez", classOf[SiteGridReducer], job)

    job.waitForCompletion(true)

  }

  def getScanForTable(tableName: String): Scan = {
    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
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
      val gridId = result.getValue(SiteGridDAO.getCF, SiteGridDAO.GRID_ID)

      val hasArea = wkbReader.read(result.getValue(SiteGridDAO.getCF, SiteGridDAO.GEOM)).getArea > 0

      //Emit either a country (not designated) or a designated area
      if (hasArea) {
        emittedKey.set(gridId)
        context.write(emittedKey, result)
      }

    }
  }


  /**
   *
   */
  class SiteGridReducer extends TableReducer[ImmutableBytesWritable, Result, ImmutableBytesWritable] {

    val CF = "cfv".getBytes


    override def reduce(key: ImmutableBytesWritable, values: Iterable[Result],
                        context: Reducer[ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation]#Context): Unit = {


      val (eGrids, sGrids) =
        values.map(r => SiteGridDAO.fromResult(r))
          .partition(f => f.iucnCat.equals(EEZ))

      for (eG <- eGrids) {

        for (sG <- sGrids) {

          val localOp = OperatorIntersection.local()
          val outGeoms = localOp.execute(new SimpleGeometryCursor(eG.geom), new SimpleGeometryCursor(sG.geom), sr, null, 4)

          val ixed = Iterator.continually(outGeoms.next).takeWhile(_ != null)

          for (ix <- ixed) {

            val put: Put = getOutput(eG)
            put.add(CF, "ee_id".getBytes, Bytes.toBytes(eG.siteId))
            put.add(CF, "site_id".getBytes, Bytes.toBytes(sG.siteId))
            put.add(CF, "grid_id".getBytes, Bytes.toBytes(eG.gridId))

            val wkb = wkbExport.execute(WkbExportFlags.wkbExportDefaults, ix, null).array()
            put.add(CF, "geom".getBytes, wkb)
            context.write(null, put)
          }
        }
      }
    }

    def getOutput(cG: TSiteGrid): Put = {
      val nextBytes = new Array[Byte](4)
      Random.nextBytes(nextBytes)
      val suffix = Bytes.toBytes(cG.gridId) ++ Bytes.toBytes(cG.siteId)
      //a put with a very paranoid key
      val put = new Put(nextBytes ++ suffix)
      put
    }
  }

}

