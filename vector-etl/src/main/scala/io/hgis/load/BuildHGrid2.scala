package io.hgis.load

import java.math.BigInteger
import javax.persistence.{EntityManager, Persistence}

import com.vividsolutions.jts.geom.{Polygon, Geometry}
import com.vividsolutions.jts.geom.util.AffineTransformation
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import com.vividsolutions.jts.{geom => jts}
import io.hgis.domain.GridNode

import scala.collection.JavaConversions._


/**
 * Builds a hierarchical grid based on the data density
 *
 * Created by willtemperley@gmail.com on 24/10/2014.
 */
object BuildHGrid2 {

  val writer = new WKBWriter

  val westernHemisphere: String = "POLYGON((-180 -90,-180 90,0 90,0 -90,-180 -90))"
  val easternHemisphere: String = "POLYGON((0 -90,0 90,180 90,180 -90,0 -90))"

  var startVal = 100000
  val pointCountThreshold: Int = 10000

  def getNextId: Int = {
    startVal += 1
    startVal
  }

  def main(args: Array[String]) {

    val emf = Persistence.createEntityManagerFactory("grid-domain")
    val em = emf.createEntityManager()


    val hemisphere = if (args(0) == "W") westernHemisphere else easternHemisphere

    em.getTransaction.begin()
    val geometry = geomFromText(hemisphere)

    val startNode = new GridNode
    startNode.jtsGeom = geometry
    startNode.gridId = startVal

    divide(startNode, em)
    em.getTransaction.commit()

  }

  def divide(g: GridNode, em: EntityManager) {

    val env = g.jtsGeom.getEnvelopeInternal

    if (g.jtsGeom.getArea < 2000) {

      /* Base case - find the worst-case example of an entity. */
      val sql =
        """
        SELECT count(*) from hgrid.point_dump p where
        p.geom && st_setsrid(st_makebox2d(st_makepoint(:minX, :minY), st_makepoint(:maxX, :maxY)), 4326)
        group by entity_type_id, entity_id
        order by 1 desc limit 1
        """.stripMargin

      val q = em.createNativeQuery(sql)
        .setParameter("minX", env.getMinX)
        .setParameter("minY", env.getMinY)
        .setParameter("maxX", env.getMaxX)
        .setParameter("maxY", env.getMaxY)

//      val pointCount = q

      val results = q.getResultList

      val pointCount: BigInt = if (!results.isEmpty) results.head.asInstanceOf[BigInteger] else 0

      println(env)
//      if (pointCount.is)

//        .asInstanceOf[BigInteger]

      //      println(g.getArea)
      if (pointCount < pointCountThreshold) {
        println(pointCount)
        g.isLeaf = true
//        val gridCell = new GridCell
//        gridCell.jtsGeom = g.asInstanceOf[Polygon]
        if (g.gridId % 50 == 0) {
          em.getTransaction.commit()
          println("committing")
          em.getTransaction.begin()
        }

        g.jtsGeom.setSRID(4326)
        em.persist(g)
        return
      }
    }

    //Persisting everything, but NB base case is treated differently.
    g.jtsGeom.setSRID(4326)
    em.persist(g)

    /* Split into 4 */
    val dX = (env.getMaxX - env.getMinX) / 2
    val dY = (env.getMaxY - env.getMinY) / 2

    val ll = new GridNode
    ll.gridId = getNextId
    ll.parentId = g.gridId
    ll.jtsGeom = AffineTransformation.scaleInstance(0.5, 0.5, env.getMinX, env.getMinY).transform(g.jtsGeom)

    val lr = new GridNode
    lr.gridId = getNextId
    lr.parentId = g.gridId
    lr.jtsGeom =  AffineTransformation.translationInstance(dX, 0).transform(ll.jtsGeom)

    val ur = new GridNode
    ur.gridId = getNextId
    ur.parentId = g.gridId
    ur.jtsGeom = AffineTransformation.translationInstance(dX, dY).transform(ll.jtsGeom)

    val ul = new GridNode
    ul.gridId = getNextId
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
