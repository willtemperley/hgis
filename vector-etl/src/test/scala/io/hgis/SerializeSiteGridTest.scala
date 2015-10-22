package io.hgis

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.{geom => jts}
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.SiteGrid
import io.hgis.hdomain.ConvertsGeometry
import io.hgis.load.DataAccess
import io.hgis.vector.domain.{SiteGridDAO, TSiteGrid}
import org.apache.hadoop.hbase.client.Get
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._

/**
  * Tests for going to HBase and back
  *
  * Created by willtemperley@gmail.com on 17-Nov-14.
  */
class SerializeSiteGridTest extends SerializationTestBase[TSiteGrid] with ConvertsGeometry {

  @Test
  def serializeAndDeserialize(): Unit = {

     val q = em.createQuery("from SiteGrid", classOf[SiteGrid]).setMaxResults(100)

     for (sg <- q.getResultList) {

       val deserialized: TSiteGrid = serializeDeserialize(sg)

       Assert.assertTrue(
         areEqual(deserialized, sg)
       )

    }

   }

  def serializeDeserialize(sg: SiteGrid): TSiteGrid = {

    val jtsGeom: Geometry = sg.jtsGeom
    sg.geom = jtsToEsri(jtsGeom)

    //It's random!  make sure you use the same one you put in to retrieve!
    val rowKey: Array[Byte] = sg.getRowKey
    val put = SiteGridDAO.toPut(obj = sg, rowKey)


    hTable.put(put)

    val res = hTable.get(new Get(rowKey))

    //FIXME
//    put.add("cft".getBytes, "highway".getBytes, "test".getBytes)
//    val str = AccessUtil.stringColumn("cft".getBytes, "highway") _
//
//    var str1 = str(res)
//    if (str1.equals("test")) {
//      println("OK")
//    }

    val deserializedPA = SiteGridDAO.fromResult(res)
    deserializedPA
  }

  override def areEqual(s1: TSiteGrid, s2: TSiteGrid): Boolean = {
      if (!s1.iucnCat.equals(s2.iucnCat)) return false
      if (!s1.isDesignated.equals(s2.isDesignated)) return false
      if (!s1.gridId.equals(s2.gridId)) return false
      super.areEqual(s1, s2)
  }
}
