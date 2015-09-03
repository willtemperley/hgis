package io.hgis.vector.mr

/**
 * Intersects PAs with themselves
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

import com.esri.core.geometry.{Geometry, OperatorImportFromWkt}
import io.hgis.ConfigurationFactory
import io.hgis.vector.domain.{SiteGridDAO, SiteOverlapDAO}
import SiteOverlapDAO.SiteOverlap
import io.hgis.op.IntersectUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper, TableReducer}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._
/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object SiteOverlapMR {


  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    //Maps the site grid
    TableMapReduceUtil.initTableMapperJob("site_grid", scan,
      classOf[SiteOverlapMR.SiteGridMapper], classOf[ImmutableBytesWritable], classOf[Result], job)
    TableMapReduceUtil.addDependencyJars(job)

    //Reduces
    TableMapReduceUtil.initTableReducerJob("site_overlap", classOf[SiteOverlapMR.SiteGridReducer], job)
    job.waitForCompletion(true)

  }


  /**
   * Simply maps a site to a grid id
   * Essentially grouping by grid_id - therefore a reducer should receive all the sites (PAs) and manage the overlap
   */
  class SiteGridMapper extends TableMapper[ImmutableBytesWritable, Result] {

    val wktImportOp = OperatorImportFromWkt.local()
    val emittedKey = new ImmutableBytesWritable()

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Result]#Context): Unit = {

      //Don't even bother getting the int from bytes - just map the record directly
      val gridId = result.getValue(SiteGridDAO.getCF, SiteGridDAO.GRID_ID)
      emittedKey.set(gridId)

      context.write(emittedKey, result)

    }
  }

  /**
   * Ingests a set of site grids and outputs their overlap
   *
   * (A triangular matrix, minus the identity)
   *
   */
  class SiteGridReducer extends TableReducer[ImmutableBytesWritable, Result, ImmutableBytesWritable] {
    override def reduce(key: ImmutableBytesWritable, values: Iterable[Result],
                        context: Reducer[ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation]#Context): Unit = {

      val sGs = values.map(r => SiteGridDAO.fromResult(r))

      for (sG1 <- sGs) {
        for (sG2 <- sGs) {
          if (sG1.siteId < sG2.siteId) {
            val iXn: Iterator[Geometry] = IntersectUtil.executeIntersect(sG1.geom, sG2.geom)

            val area = iXn.map(_.calculateArea2D()).foldLeft(0d)((r,c) => r + c)
            //context.write()

            if (area > 0) {
              //Rowkey should just be the same as the grid id ... as we're flattening the world
              val sO = new SiteOverlap
              sO.siteId1 = sG1.siteId
              sO.siteId2 = sG2.siteId
              sO.area = area
              val put = SiteOverlapDAO.toPut(sO, key.get())
              context.write(null, put)
            }

          }
        }
      }
    }
  }

}

