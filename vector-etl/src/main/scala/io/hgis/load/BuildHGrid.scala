package io.hgis.load

import java.math.BigInteger
import javax.persistence.{EntityManager, Persistence}

import com.vividsolutions.jts.geom.util.AffineTransformation
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import com.vividsolutions.jts.{geom => jts}
import io.hgis.domain.GridNode

/**
 * Builds a hierarchical grid based on the data density
 *
 * Created by willtemperley@gmail.com on 24/10/2014.
 */
object BuildHGrid {

  val writer = new WKBWriter

  val westernHemisphere: String = "POLYGON((-180 -90,-180 90,0 90,0 -90,-180 -90))"
  val easternHemisphere: String = "POLYGON((0 -90,0 90,180 90,180 -90,0 -90))"

  def main(args: Array[String]) {

    val emf = Persistence.createEntityManagerFactory("grid-domain")
    val em = emf.createEntityManager()


    val hemisphere = if (args(0) == "W") westernHemisphere else easternHemisphere

    em.getTransaction.begin()
    val geometry = geomFromText(hemisphere)
    divide(geometry,em)
    em.getTransaction.commit()

  }

  def divide(g: Geometry, em: EntityManager) {

    val pointCountThreshold: Int = 1500

    val env = g.getEnvelopeInternal

    if (g.getArea < 2000) {

      /* Base case */
      val sql =
        """
        SELECT count(*) from gridgis.wdpa_dumped_points p where p.geom &&
        st_setsrid(st_makebox2d(st_makepoint(:minX, :minY), st_makepoint(:maxX, :maxY)), 4326);
        """.stripMargin

      val q = em.createNativeQuery(sql)
        .setParameter("minX", env.getMinX)
        .setParameter("minY", env.getMinY)
        .setParameter("maxX", env.getMaxX)
        .setParameter("maxY", env.getMaxY)

      val pointCount = q.getSingleResult.asInstanceOf[BigInteger]

//      println(g.getArea)
      if (pointCount.intValue < pointCountThreshold) {
        println(pointCount)
        val hG = new GridNode
        hG.jtsGeom = g.asInstanceOf[Polygon]
        em.persist(hG)
        return
      }
    }

    /* Otherwise, split into 4 */
    val dX = (env.getMaxX - env.getMinX) / 2
    val dY = (env.getMaxY - env.getMinY) / 2

    val ll: jts.Geometry = AffineTransformation.scaleInstance(0.5, 0.5, env.getMinX, env.getMinY).transform(g)
    val lr = AffineTransformation.translationInstance(dX, 0).transform(ll)
    val ur = AffineTransformation.translationInstance(dX, dY).transform(ll)
    val ul = AffineTransformation.translationInstance(0, dY).transform(ll)

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
