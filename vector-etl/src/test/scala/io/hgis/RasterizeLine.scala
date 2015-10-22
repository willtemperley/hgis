package io.hgis

import javax.persistence.EntityManager

import _root_.io.hgis.domain.osm.Way
import _root_.io.hgis.inject.JPAModule
import com.google.inject.Guice
import com.vividsolutions.jts.geom.GeometryFactory
import io.hgis.dump.ShapeWriter
import io.hgis.hgrid.GlobalGrid
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.Test

import scala.collection.JavaConversions._

/**
 * Created by willtemperley@gmail.com on 26-May-15.
 */
class RasterizeLine {


//  val grid = new GlobalGrid(43200, 21600)
  val grid = new GlobalGrid(4320, 2160, 512)
  val injector = Guice.createInjector(new JPAModule)
  val em = injector.getInstance(classOf[EntityManager])

  @Test
  def doSomething(): Unit = {

    val crs = DefaultGeographicCRS.WGS84 // set crs first
    val env = new ReferencedEnvelope(-8, -6, 57, 58, crs)

    val ways = getQ(env)

    val sw = new ShapeWriter()

    for ( way <- ways ) {

//      val pts = way.geom.getCoordinates.map(f => grid.snap(f.getOrdinate(0), f.getOrdinate(1))).toSet
//
//
//      for (pt <- pts) {
//        val geoPt = grid.unSnap(pt._1, pt._2)
//        val sf = sw.addFeature(geoPt, Seq(pt._1, pt._2))
//        sf
//      }

//      val en
//      GeoHash.coverBoundingBox()


      //      geom.getBoundary.queryEnvelope2D(envelope2D)

      //      val upperLeft = envelope2D.getMinX
      //      val bottomRight = envelope2D.getMaxX
      //      val coverage = GeoHash.coverBoundingBox(upperLeft.y, upperLeft.x, bottomRight.y, bottomRight.x)

    }

    sw.write("E:/tmp/test.shp")
//    val v = grid.v

//    for (f <-v) {
//      if (f > 0)
//        println(f)
//    }

//    RasterWriter.paint(grid.w, grid.h, grid.v)

//    grid.snap()
//    val image: BufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
//    val graphics: Graphics2D = image.createGraphics
//    val screenArea: Rectangle = new Rectangle(0, 0, width, height)
//
//
//    baos.write()
//
//    ImageIO.write(image, "png", image.wr)

  }

  def getQ(referencedEnvelope: ReferencedEnvelope): List[Way] = {
    val gf = new GeometryFactory
    val g = gf.toGeometry(referencedEnvelope)
    g.setSRID(4326)
    val q =em.createQuery("from Way w where intersects(w.geom, :filter) = true", classOf[Way]).setParameter("filter", g)
    q.getResultList.toList
  }

}
