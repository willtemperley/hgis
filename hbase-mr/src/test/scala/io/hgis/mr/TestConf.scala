package io.hgis.mr

import io.hgis.ConfigurationFactory
import org.junit.{Assert, Test}

/**
 * Created by willtemperley@gmail.com on 07-Jul-15.
 */
class TestConf {

  @Test
  def loadConfiguration(): Unit = {

    val c = ConfigurationFactory.get

    Assert.assertTrue(c.get("hbase.master") != null)

  }

}
