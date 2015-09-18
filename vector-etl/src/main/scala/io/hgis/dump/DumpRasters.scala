package io.hgis.dump

import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.scanutil.TableIterator
import org.apache.hadoop.hbase.client.{HTable, Scan}

/**
  * Created by willtemperley@gmail.com on 05-Jun-15.
  *
  */
object DumpRasters extends TableIterator {

   val wktReader = new WKTReader()

   def getGeom = AccessUtil.geomColumn(new WKBReader(), "cfv") _

   def getId = AccessUtil.intColumn("cfv", "id") _

   def main(args: Array[String]): Unit = {

     val htable = new HTable(ConfigurationFactory.get, "ways")

 //    val scan = new Scan
 //    scan.addFamily("cfv".getBytes)
     val scanner = htable.getScanner(getScan)

     val ways = getIterator(scanner)

     val strs = ways.map(f => (getGeom, getId))//.foreach(println)

 //    strs.foreach(println)
 //    val wktR = new WKTReader()

     for (s <- strs ) {
     }

 //    val sw = new ShapeWriter("LineString", 4326)
 //
 //    ways.foreach(f => sw.addFeature(getGeom(f), Seq(0,0)))
 //
 //    sw.write("target/test.shp")


   }
   def getScan: Scan = {

     val s1 = new Scan()
     s1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "ways".getBytes)
     s1.addFamily("cfv".getBytes)
     s1.addColumn("cfv".getBytes, "wkt".getBytes)
     s1.addColumn("cfv".getBytes, "id".getBytes)
     s1.setMaxResultsPerColumnFamily(100)
     s1

     //    val s2 = new Scan()
     //    s2.addColumn("cfv".getBytes, "site_id".getBytes)
     //    s2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "pa_grid".getBytes())

 //    DebugGeom.encodeScan(s1)// + "," + encodeScan(s2)
   }
 }
