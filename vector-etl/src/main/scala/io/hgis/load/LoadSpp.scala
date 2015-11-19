package io.hgis.load

import com.vividsolutions.jts.geom.{Geometry, MultiPolygon}
import io.hgis.domain.{LoadQueue, EcoregionEEZ}
import io.hgis.domain.rl.Species
import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer

/**
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
class LoadSpp extends GridLoader[Species](classOf[Species]) {

  val CF: Array[Byte] = "cfv".getBytes

  override def getIds: Iterable[Any] = {
    //TAKE CARE
    //Entity ID is a misnomer here, as we've used a synthetic id when splitting multi polys
    val q = em.createNativeQuery(
      """
      select ogc_fid from hgrid.species where ogc_fid not in
      (select entity_id from hgrid.load_queue where entity_type='sp')
      limit 1000
      """.stripMargin
    )
    q.getResultList.map(_.asInstanceOf[Int].toLong)
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

      val gridCells = getHGrid(analysisUnit.jtsGeom.getEnvelopeInternal)
      /*
      This hugely increases performance - splitting single polys into multi polys
       */
      for (p <- geoms) {
        analysisUnit.jtsGeom = p
        executeGridding(analysisUnit, gridCells, buffer = lb)
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

  override def notifyComplete(obj: Species): Unit = {

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

  override def addColumns(put: Put, obj: Species): Unit = {

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
