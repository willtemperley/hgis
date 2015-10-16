package io.hgis

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.{geom => jts}
import io.hgis.domain.SiteGrid
import io.hgis.load.{ConvertsGeometry, DataAccess}
import io.hgis.vector.domain.{SiteGridDAO, TSiteGrid}
import org.apache.hadoop.hbase.client.Get
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._

/**
  * Tests for going to HBase and back
  *
  * Created by willtemperley@gmail.com on 17-Nov-14.
  */
class SerializeSiteGridTest extends ConvertsGeometry {


   val em = DataAccess.em

  @Test
   def serializeAndDeserialize(): Unit = {

     val q = em.createQuery("from SiteGrid", classOf[SiteGrid]).setMaxResults(100)

     for (sg <- q.getResultList) {

       val jtsGeom: Geometry = sg.jtsGeom
       sg.geom = jtsToEsri(jtsGeom)

       //It's random!  make sure you use the same one you put in to retrieve!
       val rowKey: Array[Byte] = sg.getRowKey
       val put = SiteGridDAO.toPut(obj = sg, rowKey)

       val hTable = MockHTable.create()
       hTable.put(put)

       val res = hTable.get(new Get(rowKey))

       val deserializedPA = SiteGridDAO.fromResult(res)


       Assert.assertTrue(
         siteGridsEqual(deserializedPA, sg)
       )

     }

     def siteGridsEqual(s1: TSiteGrid, s2: TSiteGrid): Boolean = {

       if (!s1.geom.equals(s2.geom)) return false

       if (!s1.iucnCat.equals(s2.iucnCat)) return false

       if (!s1.isDesignated.equals(s2.isDesignated)) return false

       if (!s1.gridId.equals(s2.gridId)) return false

       if (!s1.entityId.equals(s2.entityId)) return false

       true
     }


   }


 }
