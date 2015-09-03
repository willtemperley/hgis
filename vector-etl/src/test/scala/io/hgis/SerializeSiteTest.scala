package io.hgis

import java.util

import com.vividsolutions.jts.geom.MultiPolygon
import com.vividsolutions.jts.{geom => jts}
import io.hgis.domain.{GridCell, Site}
import io.hgis.load.{ConvertsGeometry, DataAccess, LoadSites}
import io.hgis.vector.domain.{SiteDAO, TSite}
import org.apache.hadoop.hbase.client.{Get, Put}
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Tests for going to HBase and back
 *
 * Created by willtemperley@gmail.com on 17-Nov-14.
 */
class SerializeSiteTest  extends ConvertsGeometry {


  val em = DataAccess.em

  @Test
  def randomBytes(): Unit = {
    val hTable = MockHTable.create()

    val nextBytes = new Array[Byte](8)
    Random.nextBytes(nextBytes)
    val put = new Put(nextBytes)
    hTable.put(put)
  }

  @Test
  def serializeAndDeserialize(): Unit = {

    val query = em.createNativeQuery("SELECT id, geom, name, iucn_cat, is_designated " +
      "FROM protected_sites.wdpa_latest_all where geom && (select geom from administrative_units.country where isoa3_id = 'KEN')", classOf[Site])
    val protectedAreas = query.getResultList.asInstanceOf[util.ArrayList[Site]]

    for (pa <- protectedAreas) {

      println(pa.name)
      val hGrid: util.ArrayList[GridCell] = LoadSites.getHGrid(em, pa)

      val jtsGeom: MultiPolygon = pa.jtsGeom
      pa.geom = jtsToEsri(jtsGeom)
      pa.gridCells = hGrid.map(f => jtsWktWriter.write(f.jtsGeom)).toArray
      pa.gridIdList = hGrid.map(f => f.gridId.toString).toArray

      //It's random!  make sure you use the same one you put in to retrieve!
      val rowKey: Array[Byte] = pa.getRowKey
      val put = SiteDAO.toPut(obj = pa, rowKey)

      val hTable = MockHTable.create()
      hTable.put(put)

      val res = hTable.get(new Get(rowKey))

      val deserializedPA = SiteDAO.fromResult(res)


      Assert.assertTrue(
        sitesEqualEsri(deserializedPA, pa)
      )

    }

    def sitesEqualEsri(s1: TSite, s2: TSite): Boolean = {

      if (!s1.geom.equals(s2.geom)) return false

      if (!s1.iucnCat.equals(s2.iucnCat)) return false

      if (!s1.gridCells.sameElements(s2.gridCells)) return false

      if (!s1.gridIdList.sameElements(s2.gridIdList)) return false

      //      if (s2.siteId.equals(null.asInstanceOf[Int])) return false

      if (!s1.siteId.equals(s2.siteId)) return false

      if (!s1.isDesignated.equals(s2.isDesignated)) return false

      return true


    }


  }


}
