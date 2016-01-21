package io.hgis.load

import com.esri.core.geometry.WkbExportFlags
import io.hgis.domain.LoadQueue
import io.hgis.domain.rl.{TestSpecies, Species}
import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

/**
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
class LoadTestSppSimple extends GridLoader[TestSpecies](classOf[TestSpecies]) {

  val CF: Array[Byte] = "cfv".getBytes

  override def getIds: Iterable[Any] = {
    //TAKE CARE
    //Entity ID is a misnomer here, as we've used a synthetic id when splitting multi polys
    val q = em.createNativeQuery(
      """
      select ogc_fid from hgrid.test_species;
      """.stripMargin
    )
    q.getResultList.map(_.asInstanceOf[Int].toLong)
  }

  override def executeLoad(table: HTableInterface) {

    val ids = getIds.toList
    val progressMonitor = new ProgressMonitor(ids.size)

    table.setAutoFlushTo(true)

    for (analysisUnitId <- ids.zipWithIndex) {

      val analysisUnit = getEntity(analysisUnitId._1)
      analysisUnit.geom = jtsToEsri(analysisUnit.jtsGeom)
      if(analysisUnit == null) {
        throw new RuntimeException(clazz.getSimpleName + " was not found with id: " + analysisUnitId)
      }

      val put = new Put(Bytes.toBytes(analysisUnit.ogcFid).reverse)

      addColumns(put, analysisUnit)

      table.put(put)

      notifyComplete(analysisUnit)

      progressMonitor.updateProgress(analysisUnitId._2)

    }
  }

  override def notifyComplete(obj: TestSpecies): Unit = {

    //    paLoaded.id
    //      = obj.entityId

//    val loadRecord = new LoadQueue
//    loadRecord.entityId = obj.ogcFid
//    loadRecord.entityType = "sp"
//    loadRecord.isLoaded = true
//
//    em.getTransaction.begin()
//    em.persist(loadRecord)
//    em.getTransaction.commit()
  }

  override def addColumns(put: Put, obj: TestSpecies): Unit = {

    val geom: Array[Byte] = esriWkbWriter.execute(WkbExportFlags.wkbExportDefaults, obj.geom, null).array()
    put.add(CF, GEOM, geom)

    put.add(CF, "ogc_fid".getBytes, Bytes.toBytes(obj.ogcFid))
    put.add(CF, "entity_id".getBytes, Bytes.toBytes(obj.entityId))
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
