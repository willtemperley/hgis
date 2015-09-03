package io.hgis.load

import com.esri.core.geometry.Geometry
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
object DirectLoad extends ConvertsGeometry {


  def main(args: Array[String]) {

    val q = DataAccess.em.createNativeQuery("select wdpa_id from gridgis.worst_100")// where wdpa_id = 20615")
    val conf = DataAccess.injector.getInstance(classOf[Configuration])

    //FIXME hardcode table
    val hTable = new HTable(conf, "site_grid")

    val results = q.getResultList

    for (wdpaId <- results) {

      val site = DataAccess.getSite(wdpaId.toString.toInt)
      println(site.name)
      site.geom = jtsToEsri(site.jtsGeom)

      val gridCells = LoadSites.getHGrid(DataAccess.em, site)
      val gridGeoms: Array[Geometry] = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
      val gridIds: Array[Int] = gridCells.map(f => f.gridId.toInt).toArray

      val siteGrids = IntersectUtil.executeIntersect(site.geom, gridGeoms, gridIds)

      siteGrids.foreach(_.siteId = site.siteId)

      for (sg <- siteGrids ) {
        val put = SiteGridDAO.toPut(sg, sg.getRowKey)
        hTable.put(put)
      }

      hTable.flushCommits()

      println(site.name + " done.")

    }
    //    sgs.foreach(println)

  }


}
