package io.hgis.load

import java.math.BigInteger
import javax.persistence.{EntityManager, Persistence}

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.util.AffineTransformation
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import com.vividsolutions.jts.{geom => jts}
import io.hgis.domain.GridNode

/**
 * Builds a hierarchical grid based on the data density
 *
 * Created by willtemperley@gmail.com on 24/10/2014.
 */
object ReBuildHGrid {

  val writer = new WKBWriter

//  val world: String = "POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))"
  val easternHemisphere: String = "POLYGON((0 -90,0 90,180 90,180 -90,0 -90))"
  val westernHemisphere: String = "POLYGON((-180 -90,-180 90,0 90,0 -90,-180 -90))"

  var startVal = 0
  val pointCountThreshold: Int = 1500

  def main(args: Array[String]) {

    val emf = Persistence.createEntityManagerFactory("grid-domain")
    val em = emf.createEntityManager()


    val hemisphere = if (args(0) == "W") westernHemisphere else easternHemisphere

    em.getTransaction.begin()
    val geometry = geomFromText(hemisphere)

    val startNode = new GridNode
    startNode.jtsGeom = geometry
    startNode.gridId = startVal
    startNode.parentId = 0

    divide(startNode, em)
    em.getTransaction.commit()

  }

  def divide(g: GridNode, em: EntityManager) {

    val env = g.jtsGeom.getEnvelopeInternal

    if (g.jtsGeom.getArea < 2000) {
      /* Base case */
      val sql = "SELECT count(*) from hgrid.h_grid_centroid hg where hg.geom && st_setsrid(st_makebox2d(st_makepoint(:minX, :minY), st_makepoint(:maxX, :maxY)), 4326);"
      val q = em.createNativeQuery(sql)
        .setParameter("minX", env.getMinX)
        .setParameter("minY", env.getMinY)
        .setParameter("maxX", env.getMaxX)
        .setParameter("maxY", env.getMaxY)

      val pt_count = q.getSingleResult.asInstanceOf[BigInteger]

//      println(g.getArea)
      if (pt_count.intValue() == 1) {
        //found the orignal leaf. now, need the ID.

        val sql = "SELECT id from hgrid.h_grid_centroid hg where hg.geom && st_setsrid(st_makebox2d(st_makepoint(:minX, :minY), st_makepoint(:maxX, :maxY)), 4326);"
        val q = em.createNativeQuery(sql)
          .setParameter("minX", env.getMinX)
          .setParameter("minY", env.getMinY)
          .setParameter("maxX", env.getMaxX)
          .setParameter("maxY", env.getMaxY)
        val originalId = q.getSingleResult.asInstanceOf[Integer]

        g.isLeaf = true
        g.gridId = originalId
        //already have the parent id
        em.persist(g)

        if (originalId % 1000 == 0) {
          println("gridid: " + g.gridId)
          em.getTransaction.commit()
          em.getTransaction.begin()
        }

        return
      }
    }

    //Persisting everything, but NB base case is treated differently.
    em.persist(g)

    /* Split into 4 */
    val dX = (env.getMaxX - env.getMinX) / 2
    val dY = (env.getMaxY - env.getMinY) / 2

    startVal += 1
    val ll = new GridNode
    ll.gridId = startVal
    ll.parentId = g.gridId
    ll.jtsGeom = AffineTransformation.scaleInstance(0.5, 0.5, env.getMinX, env.getMinY).transform(g.jtsGeom)

    startVal += 1
    val lr = new GridNode
    lr.gridId = startVal
    lr.parentId = g.gridId
    lr.jtsGeom =  AffineTransformation.translationInstance(dX, 0).transform(ll.jtsGeom)

    startVal += 1
    val ur = new GridNode
    ur.gridId = startVal
    ur.parentId = g.gridId
    ur.jtsGeom = AffineTransformation.translationInstance(dX, dY).transform(ll.jtsGeom)

    startVal += 1
    val ul = new GridNode
    ul.gridId = startVal
    ul.parentId = g.gridId
    ul.jtsGeom = AffineTransformation.translationInstance(0, dY).transform(ll.jtsGeom)

    /* Recurse */
    divide(ll, em)
    divide(lr, em)
    divide(ul, em)
    divide(ur, em)

  }

  def printElapsedTime(t0: Long): Unit = {
    println("Time elapsed: " + (System.currentTimeMillis() - t0) / 1000)
  }


  def geomFromText(wktString: String): Geometry = {
    val wkt = new WKTReader
    wkt.read(wktString)
  }


}
