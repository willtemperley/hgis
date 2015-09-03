package io.hgis.osm

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

import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import io.hgis.hgrid.GlobalGrid
import io.hgis.osmdomain.WayDAO
import io.hgis.ConfigurationFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper, TableReducer}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object HighwayDensity {

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
//    scan.addFamily("cft".getBytes)

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    TableMapReduceUtil.addDependencyJars(job)

//    TableMapReduceUtil.initTableMapperJob("ways", scan,
//      classOf[HighwayDensity.WayMapper], classOf[ImmutableBytesWritable], classOf[IntWritable], job)

    TableMapReduceUtil.initTableReducerJob("hashcount", classOf[HighwayDensity.WayReducer], job)
    job.waitForCompletion(true)

  }

  /**
   *
   *
   */
  class WayMapper extends TableMapper[ImmutableBytesWritable, IntWritable] {

    //    val wkbImportOp = OperatorImportFromWkb.local()
    val wkbReader = new WKBReader()
    val emittedKey = new ImmutableBytesWritable()
    val intWritable = new IntWritable()
    intWritable.set(1)
    val grid = new GlobalGrid(4320, 2160, 512)

    /*
    Pretty much like the wordcount example! Snaps a point to a grid and writes that out with the dummy 1 value.

    Could de-dup here - many points will snap to the same grid value.
     */
    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, IntWritable]#Context): Unit = {

      val wkb = result.getValue(WayDAO.getCF, WayDAO.GEOM)

      //      val hwy = result.getValue("cft".getBytes, "highway".getBytes)
      if (wkb != null){// && hwy != null) {

        val way = wkbReader.read(wkb)
        val pts = way.getCoordinates.map(f => grid.snap(f.getOrdinate(0), f.getOrdinate(1)))

        for (p <- pts) {
          emittedKey.set(pointToBytes(p._1, p._2))
          context.write(emittedKey, intWritable)
        }

      }
    }
  }

  def pointToBytes(x: Int, y: Int): Array[Byte] = Bytes.toBytes(x) ++ Bytes.toBytes(y)

  def bytesToPoint(bytes: Array[Byte]): (Int, Int) = (Bytes.toInt(bytes.slice(0, 4)), Bytes.toInt(bytes.slice(4, 8)))

  /**
   * Simply writes out the points vs their counts.  These are "unsnapped" back geographical coordinates.
   */
  class WayReducer extends TableReducer[ImmutableBytesWritable, IntWritable, ImmutableBytesWritable] {

    val grid = new GlobalGrid(4320, 2160, 512)
    val cfv: Array[Byte] = "cfv".getBytes
    val geomCol = "geom".getBytes
    val countCol = "count".getBytes
    val wkbWriter = new WKBWriter()

    override def reduce(key: ImmutableBytesWritable, values: Iterable[IntWritable], context: Reducer[ImmutableBytesWritable, IntWritable, ImmutableBytesWritable, Mutation]#Context): Unit = {

      val ll = bytesToPoint(key.get())
      val point = grid.pixelToPoint(ll._1, ll._2)

      val put = new Put(key.get())
      put.add(cfv, geomCol, wkbWriter.write(point))

      put.add(cfv, countCol, Bytes.toBytes(values.size))

      context.write(key, put)

    }

  }

}

