package io.hgis

import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.GridNode
import io.hgis.domain.rl.{TestSpecies, Species}
import io.hgis.load.{LoadTestSppSimple, LoadSpp}
import io.hgis.scanutil.TableIterator
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.junit.{Assert, Test}

/**
  *
  * Created by willtemperley@gmail.com on 14-Oct-15.
  */
class TestLoadTestSpp extends TableIterator {

  val hTable = MockHTable.create()
  val CF = "cfv".getBytes

  val spLoader = new LoadTestSppSimple {

    override def getIds: Iterable[Any] = super.getIds.take(10)

    override def notifyComplete(analysisUnit: TestSpecies): Unit = {

    }
  }

  @Test
  def go(): Unit = {

    spLoader.executeLoad(hTable)

    val scan = new Scan

    scan.addFamily(CF)
    val scanner = hTable.getScanner(scan)
    val iterator = getIterator(scanner)
    iterator.foreach(verify)

  }

  def ogcFidCol = AccessUtil.longColumn(CF, "ogc_fid") _
  def getEntityId = AccessUtil.longColumn(CF, "entity_id") _

  def getGeom = AccessUtil.geomColumn(new WKBReader, CF, "geom") _

  def verify(res: Result): Unit = {

    val ogcFid = ogcFidCol(res)
    val entityId = getEntityId(res)
    println(entityId)
    val geomType = getGeom(res).getGeometryType

    val ids = spLoader.getIds.map(_.asInstanceOf[Long])
    Assert.assertTrue(ids.toList.contains(ogcFid))

    Assert.assertTrue(geomType.equals("Polygon") || geomType.equals("MultiPolygon"))

  }
}
