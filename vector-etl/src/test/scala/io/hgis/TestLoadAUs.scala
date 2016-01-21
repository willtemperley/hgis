package io.hgis

import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.hdomain.GriddedObjectDAO
import io.hgis.load.{LoadAUs, LoadTestPAs}
import io.hgis.scanutil.TableIterator
import io.hgis.vector.domain.SiteDAO
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.junit.{Assert, Test}

/**
 *
 * Created by willtemperley@gmail.com on 14-Oct-15.
 */
class TestLoadAUs extends TableIterator {

  val hTable = MockHTable.create()
  val CF = "cfv".getBytes

  val lpas = new LoadAUs {
    override def getIds = Array(97l, 98l)
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
  def getCountryId = AccessUtil.stringColumn(CF, "country_id") _
  def getGeom = AccessUtil.geomColumn(new WKBReader, CF, "geom") _

  def verify(res: Result): Unit = {


    val obj = GriddedObjectDAO.fromResult(res)

    val entityId = getEntityId(res)
    val cId = getCountryId(res)
    val geomType = getGeom(res).getGeometryType

    Assert.assertTrue(lpas.getIds.contains(entityId))

    Assert.assertTrue(geomType.equals("Polygon") || geomType.equals("MultiPolygon"))

    println(cId)
//    Assertt.assertTrue(catId > 0 && catId < 10)


  }
}
