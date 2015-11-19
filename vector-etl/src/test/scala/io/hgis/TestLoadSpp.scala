package io.hgis

import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.LoadQueue
import io.hgis.domain.rl.Species
import io.hgis.load.{LoadSpp, DirectLoadPAs}
import io.hgis.scanutil.TableIterator
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.junit.{Assert, Test}

/**
  *
  * Created by willtemperley@gmail.com on 14-Oct-15.
  */
class TestLoadSpp extends TableIterator {

  val hTable = MockHTable.create()
  val CF = "cfv".getBytes

  val spLoader = new LoadSpp {
    override def getIds = Array(7131l)

    /*
    Called on completion of flush to hbase table
    */
    override def notifyComplete(analysisUnit: Species): Unit = {

      em.getTransaction.begin()

//      val e = new LoadQueue
//      e.id = new e.LqId(analysisUnit.entityId, "sp")
//      em.persist(e)

      em.getTransaction.commit()


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

  def getCatId = AccessUtil.intColumn(CF, "cat_id") _

  def getGeom = AccessUtil.geomColumn(new WKBReader, CF, "geom") _

  def verify(res: Result): Unit = {

    val ogcFid = ogcFidCol(res)
    val entityId = getEntityId(res)
    println(entityId)
    val geomType = getGeom(res).getGeometryType

    Assert.assertTrue(spLoader.getIds.contains(ogcFid))

    Assert.assertTrue(geomType.equals("Polygon") || geomType.equals("MultiPolygon"))


  }
}
