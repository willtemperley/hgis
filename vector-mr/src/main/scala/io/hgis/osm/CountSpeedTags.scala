package io.hgis.osm

/**
 */

import java.io.{BufferedReader, InputStreamReader}
import java.lang.Iterable

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.osmdomain.WayDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Mutation, Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableReducer, MultiTableInputFormat, TableMapReduceUtil, TableMapper}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Reducer, Job, Mapper}

import scala.io.Source
import scala.util.Random

import scala.collection.JavaConversions._

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object CountSpeedTags {

  val wkt = new WKTReader()
  val wkb = new WKBReader()
  val geomColumn  = AccessUtil.geomColumn(wkb, "cfv", "geom") _
  
  val CFT = "cft".getBytes
  val CFV = "cfv".getBytes
  
  val highwayColumn  = AccessUtil.stringColumn(CFT, "highway") _
  val speedColumn = AccessUtil.stringColumn(CFT, "maxspeed") _

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val scan: Scan = new Scan
    scan.addFamily(CFV)
    scan.addFamily(CFT)
    scan.setFilter(new SingleColumnValueFilter(CFV, "geom".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null)))
    scan.setFilter(new SingleColumnValueFilter(CFT, "highway".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null)))
    scan.setFilter(new SingleColumnValueFilter(CFT, "maxspeed".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null)))

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    TableMapReduceUtil.addDependencyJars(job)

    TableMapReduceUtil.initTableMapperJob("transport", scan,
      classOf[CountSpeedTags.WayMapper], classOf[Text], classOf[IntWritable], job)

    TableMapReduceUtil.initTableReducerJob("osm_speed_tags", classOf[WayReducer], job)
    job.waitForCompletion(true)

  }


  /**
   *
   *
   */
  class WayMapper extends TableMapper[Text, IntWritable] {

    val wkbReader = new WKBReader()

    val keyOut = new Text()
    val valOut = new IntWritable(1)

    val wkt = new WKTReader

    var lines: List[(String, Geometry)] = null


    override def setup(context: Mapper[ImmutableBytesWritable, Result, Text, IntWritable]#Context): Unit = {

      val pt = new Path("hdfs:/user/tempehu/country.txt")
      val fs = FileSystem.get(new Configuration())

      val br=new BufferedReader(new InputStreamReader(fs.open(pt)))
//      lines = Iterator.continually(br.readLine()).takeWhile(_ != null)
//        .map(_.split("\t"))
//        .map(f => (f(0), wkt.read(f(1)), wkt.read(f(2)))).toList
      lines = Iterator.continually(br.readLine()).takeWhile(_ != null)
        .map(_.split("\t"))
        .map(f => (f(0), wkt.read(f(1)))).toList

      WayDAO
    }


    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, Text, IntWritable]#Context): Unit = {

      val wayGeom = geomColumn(result)

      for (line <- lines) {
        if (wayGeom.getEnvelope.intersects(line._2)) {
//          if (wayGeom.intersects(line._3)) {

            val hwy = highwayColumn(result)
            val spd = speedColumn(result)

            keyOut.set(line._1 + "_" + hwy + "_" + spd)
            context.write(keyOut, valOut)
//          }
        }
      }

    }

  }

  class WayReducer extends TableReducer[Text, IntWritable, ImmutableBytesWritable] {

    val keyOut = new ImmutableBytesWritable()

    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, ImmutableBytesWritable, Mutation]#Context): Unit = {


      val bytes = new Array[Byte](8)
      Random.nextBytes(bytes)
      keyOut.set(bytes)
      val put = new Put(bytes)

      val cols = key.toString.split("_")


      put.add(CFV, "country".getBytes, Bytes.toBytes(cols(0)))
      put.add(CFV, "highway".getBytes, Bytes.toBytes(cols(1)))
      put.add(CFV, "maxspeed".getBytes, Bytes.toBytes(cols(2)))
      put.add(CFV, "count".getBytes, Bytes.toBytes(values.size))
      context.write(keyOut, put)

    }
  }
}

