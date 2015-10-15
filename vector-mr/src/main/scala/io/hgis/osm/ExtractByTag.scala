package io.hgis.osm

/**
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
object ExtractByTag {

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
    scan.addFamily("cft".getBytes)

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    TableMapReduceUtil.addDependencyJars(job)

    TableMapReduceUtil.initTableMapperJob("transport", scan,
      classOf[ExtractByTag.WayExtractMapper], classOf[ImmutableBytesWritable], classOf[Put], job)

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
//    val env = wkt.read("POLYGON((25 -0.4,25 0.6,26 0.6,26 -0.4,25 -0.4))")
//
//    env.setSRID(4326)


    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      val wkb = result.getValue(WayDAO.getCF, WayDAO.GEOM)

      val rwy = result.getValue("cft".getBytes, "railway".getBytes)
      if (rwy != null){

//        val way = wkbReader.read(wkb)

          emittedKey.set(key.get())
          val put = new Put(emittedKey.get())
          put.add(WayDAO.getCF, WayDAO.GEOM, wkb)
          put.add(WayDAO.getCF, "railway".getBytes, rwy)
          context.write(emittedKey, put)
      }
    }
  }


}

