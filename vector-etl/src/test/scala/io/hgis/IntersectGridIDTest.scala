package io.hgis

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.geom.Polygon
import io.hgis.domain.Site
import io.hgis.load.{ConvertsGeometry, DataAccess, LoadSites}
import io.hgis.op.IntersectUtil
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._
/**
 * It's not obvious how the ESRI geometry API preserves ids of intersectors
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
class IntersectGridIDTest extends  ConvertsGeometry {

  def getTestSiteGrid: Site = {

//    val bmf = 555534894
//    val cornwall = 20603
//    val penwith = 4882
//    val glamorgan = 4863
    val id = 2895719
    DataAccess.em.createQuery("from Site where id = " + id, classOf[Site]).getSingleResult
  }

  @Test
  def maintainsGridId() = {

    val site = getTestSiteGrid
    site.geom = jtsToEsri(site.jtsGeom)

    val gridCells = LoadSites.getHGrid(DataAccess.em, site)
    val gridGeoms: Array[Geometry] = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
    val gridIds: Array[Int] = gridCells.map(f => f.gridId.toInt).toArray

    val sgs = IntersectUtil.executeIntersect(site.geom, gridGeoms, gridIds)

    //map of original grid ids vs geoms
    val gridIdToGeom = gridCells.map(f => f.gridId -> f.jtsGeom).toMap

    for (ixed <- sgs) {
      val g = gridIdToGeom(ixed.gridId)

      if(ixed.geom.calculateArea2D() != 0) {
        val intersects: Boolean = g.asInstanceOf[Polygon].intersects(esriToJTS(ixed.geom))
        Assert.assertTrue(intersects)
      } else {
        println("Nothing in grid: " + ixed.gridId)
      }

    }

//    sgs.foreach(println)

  }


}
