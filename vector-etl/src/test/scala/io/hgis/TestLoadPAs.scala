package io.hgis

import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.load.{LoadTestPAs, LoadPAs, DirectLoadPAs}
import io.hgis.scanutil.TableIterator
import io.hgis.vector.domain.SiteDAO
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.junit.{Assert, Test}

/**
 *
 * Created by willtemperley@gmail.com on 14-Oct-15.
 */
class TestLoadPAs extends TableIterator {

  val hTable = MockHTable.create()
  val CF = "cfv".getBytes

  val lpas = new LoadTestPAs {
    override def getIds = Array(2978149l, 2815932l)
  }

  @Test
  def go(): Unit = {

    lpas.executeLoad(hTable)

    val scan = new Scan

    scan.addFamily(CF)
    val scanner = hTable.getScanner(scan)
    val iterator = getIterator(scanner)
    iterator.foreach(verify)

  }

  def getEntityId = AccessUtil.longColumn(CF, "entity_id") _
  def getCatId = AccessUtil.intColumn(CF, "cat_id") _
  def getGeom = AccessUtil.geomColumn(new WKBReader, CF, "geom") _

  def verify(res: Result): Unit = {

    val site = SiteDAO.fromResult(res)

    val entityId = getEntityId(res)
    val catId = getCatId(res)
    val geomType = getGeom(res).getGeometryType

    Assert.assertTrue(lpas.getIds.contains(entityId))

    Assert.assertTrue(geomType.equals("Polygon") || geomType.equals("MultiPolygon"))

    Assert.assertTrue(catId > 0 && catId < 10)


  }
}
