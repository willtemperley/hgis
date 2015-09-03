package io.hgis.hgrid

import org.junit.{Assert, Test}

/**
 * Created by willtemperley@gmail.com on 04-Jun-15.
 */
class TestGlobalGrid {

  @Test
  def ras(): Unit = {

    val grid = new GlobalGrid(4320, 2160, 1024)

    val idx = grid.toRasterIdx(0,0)

    val maxIdx: Int = 4320 * 2160
    Assert.assertTrue(idx < maxIdx)

  }

}
