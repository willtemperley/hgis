package io.hgis.osm

/**
  */

import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import io.hgis.ConfigurationFactory
import io.hgis.osmdomain.WayDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper}
import org.apache.hadoop.mapreduce.{Job, Mapper}

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object CopySpeedRows {

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
    scan.addFamily("cft".getBytes)

    val filter = new SingleColumnValueFilter("cft".getBytes, "maxspeed".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null))
    filter.setFilterIfMissing(true)
    scan.setFilter(filter)

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    TableMapReduceUtil.addDependencyJars(job)

    TableMapReduceUtil.initTableMapperJob("transport", scan,
      classOf[CopySpeedRows.WayExtractMapper], classOf[ImmutableBytesWritable], classOf[Put], job)

    TableMapReduceUtil.initTableReducerJob("speed_ways", null, job)
    job.waitForCompletion(true)

  }

  /**
   *
   *
   */
  class WayExtractMapper extends TableMapper[ImmutableBytesWritable, Put] {

    val emittedKey = new ImmutableBytesWritable()

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      emittedKey.set(key.get())
      val put = new Put(emittedKey.get())
      result.rawCells().foreach(put.add)
      context.write(emittedKey, put)

    }
  }


}

