package io.hgis.load

import java.util

import com.esri.core.geometry.{Polygon, SpatialReference, OperatorIntersection, SimpleGeometryCursor}
import com.vividsolutions.jts.geom.{Envelope, Geometry, MultiPolygon}
import io.hgis.domain.rl.TestSpecies
import io.hgis.domain.{GridCell, LoadQueue}
import io.hgis.hdomain.{AnalysisUnit, GriddedObjectDAO, GriddedEntity}
import io.hgis.op.IntersectUtil
import io.hgis.op.IntersectUtil.GriddedEntityX
import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer

/**
  *
  * Created by willtemperley@gmail.com on 21-Nov-14.
  */
class LoadSppContextSensitive(testSpecies: TestSpecies, table: HTableInterface) extends GridLoader[TestSpecies](classOf[TestSpecies]) {

  val CF: Array[Byte] = "cfv".getBytes
  val sr = SpatialReference.create(4326)
  val pointCountThreshold: Int = 12500

  def getGridsByParentId(parentId: Int): util.List[GridCell] = {
    val q = em.createQuery("from GridCell where parent_id = :pid", classOf[GridCell])
      .setParameter("pid", parentId)
    q.getResultList
  }

  def getTreeBase(geometry: Geometry): GridCell = {

    val env = geometry.getEnvelopeInternal
    //    env.getEnvelopeInternal.getmist_setsrid(
    val sql =
      """
      SELECT id from hgrid.h_grid_node
      where st_contains(geom,
      st_setsrid(st_makebox2d(st_makepoint(:minX, :minY), st_makepoint(:maxX, :maxY)), 4326)
      )

      and st_srid(geom) = 4326

      order by st_area(geom) desc
      limit 1;
      """

    val q = em.createNativeQuery(sql)
      .setParameter("minX", env.getMinX)
      .setParameter("minY", env.getMinY)
      .setParameter("maxX", env.getMaxX)
      .setParameter("maxY", env.getMaxY)

    val id = q.getFirstResult

    em.find(classOf[GridCell], id.asInstanceOf[Int])
  }

  def getLeafNodes(startNodeId: Int): util.List[GridCell] = {
    val q = em.createNativeQuery(
      s"""
        SELECT id, geom, is_leaf, parent_id
        from hgrid.get_recursive($startNodeId)
        where is_leaf
        """, classOf[GridCell])
    q.getResultList.asInstanceOf[util.List[GridCell]]
  }


  def getGrids(startNodeId: Int, gridLevel: Int): util.List[GridCell] = {
    //TAKE CARE
    //Entity ID is a misnomer here, as we've used a synthetic id when splitting multi polys
    val q = em.createNativeQuery(
      s"""
      select id from hgrid.get_recursive($startNodeId)
      where depth = $gridLevel
      """
    )
    val ids: util.List[_] = q.getResultList

    val q2 = em.createQuery("from GridCell where id in (:ids)", classOf[GridCell]).setParameter("ids", ids)
    //    Array(38134)
    q2.getResultList
  }

  override def getIds: Iterable[Any] = {
    Array(9l)
  }


  override def executeLoad(table: HTableInterface): Unit = {
    //    fixme
  }

  def executeLoad2(): Unit = {

    val treeBase = getTreeBase(testSpecies.jtsGeom)

    //FIXME why can't this be simpler
    val griddedEntity = new GriddedEntityX
    griddedEntity.entityId = testSpecies.entityId
    griddedEntity.geom = jtsToEsri(testSpecies.jtsGeom)

    doGridding(treeBase, griddedEntity)

  }

  def executeStage2(griddedEntity: GriddedEntity): Unit = {

    //Recursive q to get all leaf nodes above this level
    val gridCells = getLeafNodes(griddedEntity.gridId)

    val gridGeoms = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
    val gridIds = gridCells.map(_.gridId).toArray

    val griddedEntities = executeIntersect(griddedEntity.geom, gridGeoms, gridIds, dimensionMask)

    for (sg <- griddedEntities) {

      val put = GriddedObjectDAO.toPut(sg, getRowKey(testSpecies.entityId, sg.gridId))
      //More specialised classes can add extra columns
      //      addColumns(put, analysisUnit)
      //      buffer += put
      table.put(put)

    }
    table.flushCommits()

    //jPOINTLESS
    //    notifyComplete(testSpecies)
  }

  /**
    *
    *
    * @param geomList the intersectors (e.g. a grid)
    * @param gridIds the ids of the intersectors
    * @return
    */
  def executeIntersect(geom: com.esri.core.geometry.Geometry, geomList: Array[com.esri.core.geometry.Geometry], gridIds: Array[Int], dimensionMask: Int): List[GriddedEntity] = {

    val bigPoly = new SimpleGeometryCursor(geom)
    val inGeoms = new SimpleGeometryCursor(geomList)

    val localOp = OperatorIntersection.local()
    localOp.accelerateGeometry(geom, sr, com.esri.core.geometry.Geometry.GeometryAccelerationDegree.enumMedium)

    val outGeoms = localOp.execute(inGeoms, bigPoly, sr, null, dimensionMask)

    //    Iterator.continually(outGeoms.next).takeWhile(_ != null).map((f: Geometry) => f)

    val sgs = new ListBuffer[GriddedEntity]
    var result = outGeoms.next
    var geomId = outGeoms.getGeometryID
    while (result != null) {
      if (!result.isEmpty) {

        val sg = new GriddedEntityX

        sg.geom = result
        sg.gridId = gridIds(geomId)
        sgs.append(sg)
      }
      result = outGeoms.next
      geomId = outGeoms.getGeometryID
    }
    sgs.toList
  }


  def doGridding(gridCell: GridCell, griddedEntity: GriddedEntity): Unit = {

    val pointCount = griddedEntity.geom.asInstanceOf[Polygon].getPointCount

//    if (gridCell.isLeaf) {
//
//    }

    //BASE CASE
    if (pointCount <= pointCountThreshold || gridCell.isLeaf) {
      if (gridCell.isLeaf) println("LEAF")
      println("Points: " + pointCount)
//      executeStage2(griddedEntity)
      return
    }

    val gridCells = getGridsByParentId(gridCell.gridId)

    val gridGeoms = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
    val gridIds = gridCells.map(_.gridId).toArray

    val bigPoly = new SimpleGeometryCursor(griddedEntity.geom)
    val inGeoms = new SimpleGeometryCursor(gridGeoms)

    val localOp = OperatorIntersection.local()
    localOp.accelerateGeometry(griddedEntity.geom, sr, com.esri.core.geometry.Geometry.GeometryAccelerationDegree.enumMedium)
    val outGeoms = localOp.execute(inGeoms, bigPoly, sr, null, dimensionMask)

    //      val sgs = new ListBuffer[GriddedEntity]
    var result = outGeoms.next
    var geomId = outGeoms.getGeometryID
    while (result != null) {
      if (!result.isEmpty) {

        val sg = new GriddedEntityX

        sg.geom = result
        sg.gridId = gridIds(geomId)
        sg.entityId = griddedEntity.entityId

        doGridding(gridCells(geomId), sg)

      }
      result = outGeoms.next
      geomId = outGeoms.getGeometryID
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
