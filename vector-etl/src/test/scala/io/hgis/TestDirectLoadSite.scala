package io.hgis

import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.load.LoadPAs
import io.hgis.scanutil.TableIterator
import org.apache.hadoop.hbase.client.{Result, ResultScanner, Scan}
import org.junit.{Assert, Test}

/**
 *
 * Created by willtemperley@gmail.com on 14-Oct-15.
 */
class TestDirectLoadSite extends TableIterator {

  val hTable = MockHTable.create()

  @Test
  def go(): Unit = {

    LoadPAs.execute(hTable)

    val scan = new Scan
    scan.addFamily(LoadPAs.CF)
    val scanner = hTable.getScanner(scan)
    val iterator = getIterator(scanner)
    iterator.foreach(verify)

  }

  def getInt = AccessUtil.intColumn(LoadPAs.CF, "entity_id") _
  def getGeom = AccessUtil.geomColumn(new WKBReader, LoadPAs.CF, "geom") _

  def verify(res: Result): Unit = {

    val x = getInt(res)
    val g = getGeom(res).getGeometryType

    Assert.assertTrue(g.equals("Polygon") || g.equals("MultiPolygon"))

  }
}
