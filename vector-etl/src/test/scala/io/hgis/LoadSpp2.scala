package io.hgis

import com.vividsolutions.jts.geom.{Geometry, MultiPolygon}
import io.hgis.domain.LoadQueue
import io.hgis.domain.rl.TestSpecies
import io.hgis.load.GridLoader
import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer

import scala.collection.JavaConversions._

/**
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
class LoadSpp2 extends GridLoader[TestSpecies](classOf[TestSpecies]) {

  val CF: Array[Byte] = "cfv".getBytes

  override def getIds: Iterable[Any] = {
    //TAKE CARE
    //Entity ID is a misnomer here, as we've used a synthetic id when splitting multi polys
    val q = em.createNativeQuery(
      """
      select id from hgrid.test_species2
      where grid_level = 5
      """.stripMargin
    )
    q.getResultList.map(_.asInstanceOf[Int].toLong)

//    Array(38134)

  }

  override def executeLoad(table: HTableInterface) {

    val ids = getIds.toList
    val sz = ids.size
    var pcDone = 0

    for (analysisUnitId <- ids.zipWithIndex) {

      val analysisUnit = getEntity(analysisUnitId._1)
      if (analysisUnit == null) {
        throw new RuntimeException(clazz.getSimpleName + " was not found with id: " + analysisUnitId)
      }

      val x = analysisUnit.jtsGeom.asInstanceOf[MultiPolygon]

      val geoms: IndexedSeq[Geometry] = (0 until x.getNumGeometries).map(x.getGeometryN)

      val lb = new ListBuffer[Put]

      /*
      This hugely increases performance - splitting single polys into multi polys
       */
      for (p <- geoms) {
        analysisUnit.jtsGeom = p
        executeGridding(analysisUnit, getHGrid(p.getEnvelopeInternal), buffer = lb)
      }
      lb.foreach(table.put)
      notifyComplete(analysisUnit)


      val pc = ((analysisUnitId._2 * 100) / sz.asInstanceOf[Double]).floor.toInt

      if (pc > pcDone) {
        pcDone = pc
        print("\b\b\b\b\b\b\b\b\b\b")
        println(s"$pcDone% complete")
      }

    }
  }

  override def notifyComplete(obj: TestSpecies): Unit = {

    //    paLoaded.id
    //      = obj.entityId

    val loadRecord = new LoadQueue
    loadRecord.entityId = obj.ogcFid
    loadRecord.entityType = "sp"
    loadRecord.isLoaded = true

    em.getTransaction.begin()
    em.persist(loadRecord)
    em.getTransaction.commit()
  }

  override def addColumns(put: Put, obj: TestSpecies): Unit = {

    put.add(CF, "ogc_fid".getBytes, Bytes.toBytes(obj.ogcFid))
    if (obj.binomial != null) put.add(CF, "binomial".getBytes, Bytes.toBytes(obj.binomial))
    if (obj.kingdom != null) put.add(CF, "kingdom".getBytes, Bytes.toBytes(obj.kingdom))
    if (obj.phylum != null) put.add(CF, "phylum".getBytes, Bytes.toBytes(obj.phylum))
    if (obj.clazz != null) put.add(CF, "clazz".getBytes, Bytes.toBytes(obj.clazz))
    if (obj.order != null) put.add(CF, "order".getBytes, Bytes.toBytes(obj.order))
    if (obj.family != null) put.add(CF, "family".getBytes, Bytes.toBytes(obj.family))
    if (obj.genus != null) put.add(CF, "genus".getBytes, Bytes.toBytes(obj.genus))
    if (obj.speciesName != null) put.add(CF, "species_name".getBytes, Bytes.toBytes(obj.speciesName))
    put.add(CF, "area_km2_g".getBytes, Bytes.toBytes(obj.areaKm2g))
    put.add(CF, "area_km2_m".getBytes, Bytes.toBytes(obj.areaKm2m))

  }
}
