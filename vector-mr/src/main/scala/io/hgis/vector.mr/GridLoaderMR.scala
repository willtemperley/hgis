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
 * HADOOP_CLASSPATH=$(hbase classpath) hadoop jar target/hbase-mr-0.1-SNAPSHOT-jar-with-dependencies.jar io.hgis.mr.GridLoaderMR <src> <dest>
 *
 * Created by willtemperley@gmail.com on 17-Nov-14.
 */

import com.esri.core.geometry.{Geometry, OperatorImportFromWkt, WktImportFlags}
import io.hgis.hdomain.{GriddedEntity, GriddedObjectDAO}
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.{SiteDAO, SiteGridDAO}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, Mapper}

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object GridLoaderMR {

  def main(args: Array[String]) {

    val conf: Configuration =  new Configuration
    //    conf.set(CF_RAS_SCAN, colFam)
    //    conf.set(TABLE_NAME, tableName)
    if (args.length != 2) throw new RuntimeException("Please specify an input and output table.")

    val inTable = args(0)
    val outTable = args(1)

    val scan: Scan = new Scan
    scan.addFamily(SiteGridDAO.getCF)

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    TableMapReduceUtil.initTableMapperJob(inTable, scan, classOf[GridLoaderMR.GridIXMapper], classOf[ImmutableBytesWritable], classOf[Put], job)
    TableMapReduceUtil.addDependencyJars(job)

    /* No reducer required */
    TableMapReduceUtil.initTableReducerJob(outTable, null, job)
    job.waitForCompletion(true)

  }

  class GridIXMapper extends TableMapper[ImmutableBytesWritable, Put] {

    val wktImportOp = OperatorImportFromWkt.local()
    val outputKey = new ImmutableBytesWritable

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      val site = SiteDAO.fromResult(result)

      //Get the grids and id lists
      val gridGeoms = site.gridCells.map(f => wktImportOp.execute(WktImportFlags.wktImportDefaults, Geometry.Type.Polygon, f, null))
      val siteGrids = IntersectUtil.executeIntersect(site, gridGeoms, site.gridIdList.map(_.toInt), 4)

      for (sg: GriddedEntity <- siteGrids) {

        val put = GriddedObjectDAO.toPut(sg, GriddedObjectDAO.getRowKey(site.entityId.toInt, sg.gridId))
        put.add(GriddedObjectDAO.getCF, "cat_id".getBytes, Bytes.toBytes(site.catId))

        outputKey.set(put.getRow)
        context.write(outputKey, put)
      }
    }

  }

}
