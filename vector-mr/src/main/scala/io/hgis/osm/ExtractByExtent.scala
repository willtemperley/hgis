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

import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import io.hgis.ConfigurationFactory
import io.hgis.osmdomain.WayDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper}
import org.apache.hadoop.mapreduce.{Job, Mapper}

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object ExtractByExtent {

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
//    scan.addFamily("cft".getBytes)

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    TableMapReduceUtil.addDependencyJars(job)

    TableMapReduceUtil.initTableMapperJob("ways", scan,
      classOf[ExtractByExtent.WayExtractMapper], classOf[ImmutableBytesWritable], classOf[Put], job)

    TableMapReduceUtil.initTableReducerJob("extracted_ways", null, job)
    job.waitForCompletion(true)

  }

  /**
   *
   *
   */
  class WayExtractMapper extends TableMapper[ImmutableBytesWritable, Put] {

    val wkbReader = new WKBReader()
    val emittedKey = new ImmutableBytesWritable()
    val wkt = new WKTReader

//new Envelope(25, 26, -0.4, 0.6)
    val env =
    wkt.read("POLYGON((25 -0.4,25 0.6,26 0.6,26 -0.4,25 -0.4))")

    env.setSRID(4326)
//    val intWritable = new IntWritable()
//    intWritable.set(1)
//    val grid = new GlobalGrid(4320, 2160)


    /*
    Pretty much like the wordcount example! Snaps a point to a grid and writes that out with the dummy 1 value.

    Could de-dup here - many points will snap to the same grid value.
     */
    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      val wkb = result.getValue(WayDAO.getCF, WayDAO.GEOM)

      //      val hwy = result.getValue("cft".getBytes, "highway".getBytes)
      if (wkb != null){// && hwy != null) {

        val way = wkbReader.read(wkb)

        if(way.intersects(env)) {
          emittedKey.set(key.get())
          val put = new Put(emittedKey.get())
          put.add(WayDAO.getCF, WayDAO.GEOM, wkb)
          context.write(emittedKey, put)
        }

      }
    }
  }


}

