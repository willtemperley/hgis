package io.hgis

import com.vividsolutions.jts.io.WKBReader
import io.hgis.osm.OSMLoaderMR.WayMapper
import io.hgis.osm.OsmMapper
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentCaptor

import scala.io.Source

import org.mockito.Mockito._
import org.mockito.Matchers._

import scala.collection.JavaConversions._

/**
 *
 * Created by willtemperley@gmail.com on 05-Jun-15.
 */
class OsmMapperTest extends OsmMapper {

  val wkbReader = new WKBReader

  val lines = Source.fromFile("src/test/resources/ways_1.txt")(codec = "UTF-8").getLines().toList

  val writable = new Text

  val tagFormatExample = "\"name\"=>\"Rua Santa Edwiges\", \"highway\"=>\"residential\""


  @Test
  def ensureHighwaysOnly() = {

    val mc = classOf[Mapper[LongWritable, Text, ImmutableBytesWritable, KeyValue]#Context]

    val mapper = new WayMapper()
    val context = mock(mc)

    for (line <- lines) {
      writable.set(line)
      mapper.map(null, writable, context)
    }

    val ac = ArgumentCaptor.forClass(classOf[KeyValue])


    // Should only be called 8 times, i.e. once for each field
    verify(context, times(8)).write(any(classOf[ImmutableBytesWritable]), ac.capture())


    val written = ac.getAllValues
    written.foreach(println)

  }


  @Test
  def tagExtraction() = {

    val mapper = new WayMapper()
    val tags = mapper.getTags(tagFormatExample)

    assertTrue(tags.isDefinedAt("highway"))
    assertTrue(tags.isDefinedAt("name"))

  }

  @Test
  def go() = {

    val records = lines.map(_.split("\t"))

    for (rec <- records) {

      val hex = rec(7)

      if (!hex.isEmpty) {
        val linestring = hexToBytes(hex)
        val ls = wkbReader.read(linestring)
        println(ls.getGeometryType)
      }

      val m = getTags(rec(5))
      if (m.isDefinedAt("highway")) {
        println(m.get("highway").getOrElse())
      }

    }
    //    this.writeHexWKB()
  }
}
