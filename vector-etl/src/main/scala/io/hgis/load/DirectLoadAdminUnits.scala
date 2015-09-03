package io.hgis.load

import javax.persistence.EntityManager

import com.esri.core.geometry.Geometry
import io.hgis.domain.{AdminUnit, GridCell}
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTable

import java.util.ArrayList

import scala.collection.JavaConversions._

/**
 * It's not obvious how the ESRI geometry API preserves ids of intersectors
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
object DirectLoadAdminUnits extends ConvertsGeometry {

  def getHGrid(em: EntityManager, cId: Int) :ArrayList[GridCell] = {
    val q = em.createNativeQuery("SELECT id, geom, geohash " +
      "FROM gridgis.h_grid where geom && (select c.geom from administrative_units.country c where c.id  = " + cId + ")", classOf[GridCell])
    q.getResultList.asInstanceOf[ArrayList[GridCell]]
  }

  def main(args: Array[String]) {

    val adminUnits = DataAccess.em.createQuery("from AdminUnit where geom is not null", classOf[AdminUnit]).getResultList
    val conf = DataAccess.injector.getInstance(classOf[Configuration])

    //FIXME hardcode table
    val hTable = new HTable(conf, "country_grid")


    for (au <- adminUnits) {

      println(au.name + " being processed.")

      au.geom = jtsToEsri(au.jtsGeom)

      val gridCells = getHGrid(DataAccess.em, au.siteId)

      val gridGeoms: Array[Geometry] = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
      val gridIds: Array[Int] = gridCells.map(f => f.gridId.toInt).toArray

      val siteGrids = IntersectUtil.executeIntersect(au.geom, gridGeoms, gridIds)

      siteGrids.foreach(_.siteId = au.siteId)
      siteGrids.foreach(_.iucnCat = "")
      siteGrids.foreach(_.isDesignated = false)

      for (sg <- siteGrids ) {
        val put = SiteGridDAO.toPut(sg, sg.getRowKey)
        hTable.put(put)
      }

      hTable.flushCommits()

      println(au.name + " done.")

    }
    //    sgs.foreach(println)

  }


}
