package io.hgis.osm

import java.lang.Iterable

import com.esri.core.geometry._
import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
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
object AdminUnitIXMR {

  val COUNTRY_ID = "country_id".getBytes
  val GRID_ID = "grid_id".getBytes
  val MAXSPEED = "maxspeed".getBytes
  val HIGHWAY = "highway".getBytes
  val RAILWAY = "railway".getBytes
  val WATERWAY = "waterway".getBytes

  val CFV = "cfv".getBytes
  val CFT = "cft".getBytes

  val countryIdCol = AccessUtil.stringColumn(CFV, "country_id") _
  val gridIdCol = AccessUtil.intColumn("cfv", "grid_id") _
  val geomCol = AccessUtil.geomColumn(new WKBReader(), "cfv") _

  val sr = SpatialReference.create(4326)


  def main(args: Array[String]) {

    val conf = ConfigurationFactory.get

    val scans = List(getScanForTable("au_grid"), getScanForTable("osm_grid"))

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    //Maps the site grid
    TableMapReduceUtilFix
      .initTableMapperJob(scans,
        classOf[WayMapper],
        classOf[ImmutableBytesWritable],
        classOf[Result],
        job, addDependencyJarsB = true, initCredentials = false)
    TableMapReduceUtil.addDependencyJars(job)

    //Reduces
    TableMapReduceUtil.initTableReducerJob("speed_tag_country", classOf[WayReducer], job)

    job.waitForCompletion(true)

  }

  def getScanForTable(tableName: String): Scan = {
    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
    scan.addFamily("cft".getBytes)
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getBytes)
    scan
  }

  /**
   * Simply maps a site to a grid id
   * Essentially grouping by grid_id - therefore a reducer should receive all the sites (PAs) and manage the overlap
   */
  class WayMapper extends TableMapper[ImmutableBytesWritable, Result] {

    val wkbReader = new WKBReader

    val emittedKey = new ImmutableBytesWritable()

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Result]#Context): Unit = {
      /*
       * Everything is mapped to the grid_id, retrieved and emitted as binary
       */
      val gridId = result.getValue(SiteGridDAO.getCF, SiteGridDAO.GRID_ID)
      emittedKey.set(gridId)
      context.write(emittedKey, result)

    }
  }

  /**
   * Things to watch out for:
   *
   * Empty geometries - esri won't read them
   *
   */
  class WayReducer extends TableReducer[ImmutableBytesWritable, Result, ImmutableBytesWritable] {

    val emittedKey = new ImmutableBytesWritable()

    override def reduce(key: ImmutableBytesWritable, values: Iterable[Result],
                        context: Reducer[ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation]#Context): Unit = {

      val (cGrids, wGrids) =
        values.partition(_.getValue(CFV, COUNTRY_ID) != null)


      for (cG <- cGrids) {

        for (wG <- wGrids) {
          
          val ix = geomCol(cG).intersects(geomCol(wG))
          if(ix) {

            val nextBytes = new Array[Byte](8)
            Random.nextBytes(nextBytes)
            val put = new Put(nextBytes)

            put.add(CFV, COUNTRY_ID, cG.getValue(CFV, COUNTRY_ID))
            put.add(CFV, GRID_ID, wG.getValue(CFV, GRID_ID))

            put.add(CFT, HIGHWAY, wG.getValue(CFT, HIGHWAY))
            put.add(CFT, RAILWAY, wG.getValue(CFT, RAILWAY))
            put.add(CFT, MAXSPEED, wG.getValue(CFT, MAXSPEED))
            put.add(CFT, WATERWAY, wG.getValue(CFT, WATERWAY))

            emittedKey.set(nextBytes)
            context.write(emittedKey, put)

          }
        }


//        //Calculate protection minus country
//        val d = diffOp.execute(cG.geom, diff, sr, null)

        //Do nothing if there's no area
//        if (diff.calculateArea2D() == 0) return


      }
    }
  }

}

