package io.hgis.rasterize

import io.hgis.hgrid.GlobalGrid
import org.junit.{Assert, Test}

/**
 *
 * Created by willtemperley@gmail.com on 01-Jul-15.
 */
class GlobalGridTest {

  val grid = new GlobalGrid(51200, 25600, 1024)


  @Test
  def serializeDeserialize(): Unit = {

    val key = grid.toBytes(500,1000)

    val pix = grid.keyToPixel(key)

    Assert.assertTrue(pix._1.equals(500))
    Assert.assertTrue(pix._2.equals(1000))

  }

  @Test
  def testGrid() = {

    val pix = (200,200)

    val gridId = grid.gridId(pix._1, pix._2)

    val orig = grid.gridIdToOrigin(gridId)

    val pt = grid.pixelToPoint(orig._1, orig._2)

    Assert.assertTrue(pt.getX.equals(-180.0))
    Assert.assertTrue(pt.getY.equals( -90.0))

  }

}
