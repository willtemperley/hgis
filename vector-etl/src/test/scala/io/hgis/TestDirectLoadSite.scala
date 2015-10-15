package io.hgis

import io.hgis.load.DirectLoadSite
import org.junit.Test

/**
 * Created by willtemperley@gmail.com on 14-Oct-15.
 */
class TestDirectLoadSite {

  val hTable = MockHTable.create()

  @Test
  def go(): Unit = {

    DirectLoadSite.execute(hTable)

  }
}
