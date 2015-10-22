package io.hgis.osm

import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import io.hgis.ConfigurationFactory
import io.hgis.osmdomain.WayDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper}

import scala.util.Random

object OSMLoaderMR {

  def main(args: Array[String]) {

    if (args.length != 3) {
      println("usage: table inpath outpath")
      return
    }

    val conf: Configuration = ConfigurationFactory.get
    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[WayMapper])

    job.setInputFormatClass(classOf[TextInputFormat])

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])


    val outTable = new HTable(conf, args(0))
    FileInputFormat.addInputPath(job, new Path(args(1)))

    val outPath: Path = new Path(args(2))
    FileOutputFormat.setOutputPath(job, outPath)

    HFileOutputFormat2.configureIncrementalLoad(job, outTable)

    job.waitForCompletion(true)

    // Importing the generated HFiles into a HBase table
    //    val loader = new LoadIncrementalHFiles(conf)
    //    loader.doBulkLoad(outPath, outTable)

  }


  //  id           | bigint                      | not null
  //  version      | integer                     | not null
  //  user_id      | integer                     | not null
  //  tstamp       | timestamp without time zone | not null
  //  changeset_id | bigint                      | not null
  //  tags         | hstore                      |
  //  geom         | geometry(Point,4326)        |


  /*
   * The ImmutableBytesWritable key is used by the TotalOrderPartitioner to map it
* into the correct HBase table region.
* <p>
* The KeyValue value holds the HBase mutation information (column family,
* column, and value)
*
*
CREATE TABLE public.ways
(
  id bigint NOT NULL, 0
  version integer NOT NULL,1
  user_id integer NOT NULL,2
  tstamp timestamp without time zone NOT NULL,3
  changeset_id bigint NOT NULL,4
  tags hstore,5
  nodes bigint[],6
  geom geometry(Geometry,4326),8
)
   */
  class WayMapper extends Mapper[LongWritable, Text, ImmutableBytesWritable, KeyValue] with OsmMapper {

    val wkbReader = new WKBReader
    val wkbWriter = new WKBWriter


    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, ImmutableBytesWritable, KeyValue]#Context): Unit = {

      val fields = value.toString.split("\t")
      val id = fields(0).toLong

      val nextBytes = new Array[Byte](4)
      Random.nextBytes(nextBytes)
      hKey.set(nextBytes ++ Bytes.toBytes(id))

      val tags = getMetaTags(fields)
      val osmTags = getTags(tags.get("tags").get)
      val hex = tags.get("hex").get

      val transportTag = osmTags.isDefinedAt("highway") || osmTags.isDefinedAt("railway") || osmTags.isDefinedAt("waterway")
      if (transportTag && hex != null && !hex.isEmpty && !hex.equals("\\N")) {
//      if (osmTags.isDefinedAt("highway") && hex != null && !hex.isEmpty && !hex.equals("\\N")) {

//        writeMetaData(context, fields)

        val metaTags = tags.filter(f => f._1 != "hex").filter(f => f._1 != "tags").filter(f => f._1 != "default")
        writeTags(context, metaTags)
        writeTags(context, osmTags)


        val bytes = hexToBytes(hex)
        val kv = new KeyValue(hKey.get(), CFV, WayDAO.GEOM, bytes)
        context.write(hKey, kv)

      }
      //      val geom = wkbReader.read(bytes)
      //      val writtenBytes = wkbWriter.write(geom)
      //      val field7: String = fields(7)
      //      if (!field7.equals("\\N")) {
      //        writeWKT(wkbReader, wktWriter, context, field7)
      //      }
    }
  }

}

