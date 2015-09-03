package io.hgis.load

import java.util.ArrayList
import javax.persistence.EntityManager

import com.esri.core.geometry.Geometry
import io.hgis.domain.{EcoregionEEZ, GridCell}
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTable

import scala.collection.JavaConversions._

/**
 * It's not obvious how the ESRI geometry API preserves ids of intersectors
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
object DirectLoadEEZ extends ConvertsGeometry {

  def getHGrid(em: EntityManager, cId: Int) :ArrayList[GridCell] = {
    val q = em.createNativeQuery("SELECT id, geom, null as geohash " +
      "FROM hgrid.h_grid where geom && (select e.geom from habitats_and_biotopes.ecoregion_eez e where e.id  = " + cId + ")", classOf[GridCell])
    q.getResultList.asInstanceOf[ArrayList[GridCell]]
  }

  def main(args: Array[String]) {

    val ecoregionEEZs = DataAccess.em.createQuery("from EcoregionEEZ where geom is not null", classOf[EcoregionEEZ]).getResultList
    val conf = DataAccess.injector.getInstance(classOf[Configuration])

    //FIXME hardcode table
    val hTable = new HTable(conf, "ee_grid")

    for (ee <- ecoregionEEZs) {

//      println(ee.id + " being processed.")

      ee.geom = jtsToEsri(ee.jtsGeom)

      val gridCells = getHGrid(DataAccess.em, ee.id)

      val gridGeoms: Array[Geometry] = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
      val gridIds: Array[Int] = gridCells.map(f => f.gridId.toInt).toArray

      val siteGrids = IntersectUtil.executeIntersect(ee.geom, gridGeoms, gridIds)

      siteGrids.foreach(_.siteId = ee.id)
      //Shows it's an ecoregion in MR
      siteGrids.foreach(_.iucnCat = "E")
      siteGrids.foreach(_.isDesignated = false)

      for (sg <- siteGrids ) {
        val put = SiteGridDAO.toPut(sg, sg.getRowKey)
        hTable.put(put)
      }

      hTable.flushCommits()

      println(ee.id + " done.")

    }
    //    sgs.foreach(println)

  }


}
