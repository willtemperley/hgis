package io.hgis

import com.esri.core.geometry.Geometry
import io.hgis.domain.osm.Way
import io.hgis.load.GridLoader
import io.hgis.osmdomain.TWay
import org.apache.hadoop.hbase.client.Put
import org.junit.Test

import scala.collection.JavaConversions._

/**
 * Created by willtemperley@gmail.com on 21-Oct-15.
 */
class SerializeWayTest extends GridLoader[Way](classOf[Way], geomType = Geometry.Type.Polyline) {

  override def getIds = em.createQuery("select id from Way").setMaxResults(100).getResultList

  @Test
  def go(): Unit = {

    executeLoad(MockHTable.create())

  }

  /*
  Subclasses can override this to specialise their hbase rows
   */
  override def addColumns(put: Put, obj: Way): Unit = {
    println(obj)
  }
}
