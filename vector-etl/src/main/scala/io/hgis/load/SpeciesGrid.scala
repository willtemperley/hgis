package io.hgis.load

import java.util

import com.esri.core.geometry._
import io.hgis.ConfigurationFactory
import io.hgis.domain.rl.TestSpecies
import io.hgis.hdomain.GriddedObjectDAO.GriddedObject
import io.hgis.hdomain.{AnalysisUnit, GriddedEntity, GriddedObjectDAO}
import io.hgis.op.HTree
import io.hgis.vector.domain.TGridNode
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by willtemperley@gmail.com on 10-Dec-15.
  *
  * These class params are used so they can be mocked
  */
class SpeciesGrid(gridCells: Array[TGridNode]) extends Serializable {


  val pointCountThreshold = 12500
  val dimensionMask = 4
  val sr = SpatialReference.create(4326)

  def execute(sc: SparkContext, spp: util.List[TestSpecies]): Unit = {

    val sppRdd = sc.parallelize(spp)

//    val treeBase = hTree.findSmallestContainingNode(testSpecies.jtsGeom.getEnvelope)

    def pW = partitionWorker(gridCells, hTableName = "sp_grid") _

    sppRdd.foreachPartition(pW)
//    sppRdd.foreachPartition(worker)

  }

  def partitionWorker(gridCells: Array[TGridNode], hTableName: String)(analysisUnits: Iterator[AnalysisUnit]): Unit = {

    val hTable = new HTable(ConfigurationFactory.get, hTableName)
    def worker = objectWorker(gridCells, hTable) _
    analysisUnits.foreach(worker)

  }

//  def begin(gridCells: Array[TGridNode])(analysisUnits: Iterator[AnalysisUnit]): Unit = {
  def objectWorker(gridCells: Array[TGridNode], hTable: HTable)(analysisUnit: AnalysisUnit): Unit = {

    val hTree = new HTree(gridCells, gridCells(0))


    val startNode = hTree.findSmallestContainingNode(analysisUnit.geom)

    doGridding(startNode, analysisUnit, hTree, hTable)

  }

  def doGridding(gridCell: TGridNode, griddedEntity: AnalysisUnit, hTree: HTree, hTable: HTable ): Unit = {

//    if (griddedEntity.geom == null) {
//      griddedEntity.geom = jtsToEsri(griddedEntity.jtsGeom)
//    }

    val pointCount = griddedEntity.geom.asInstanceOf[Polygon].getPointCount

    //BASE CASE
    if (pointCount <= pointCountThreshold || gridCell.isLeaf) {

      executeStage2(griddedEntity, gridCell, hTree, hTable)
      return
    }

    val gridCells = hTree.getChildNodes(gridCell.gridId)

    val gridGeoms = gridCells.map(_.geom)
    val gridIds = gridCells.map(_.gridId)

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

        val sg = new GriddedObject

        sg.geom = result
        sg.gridId = gridIds(geomId)
        sg.entityId = griddedEntity.entityId

        doGridding(gridCells(geomId), sg, hTree, hTable)

      }
      result = outGeoms.next
      geomId = outGeoms.getGeometryID
    }
  }

  def executeStage2(griddedEntity: AnalysisUnit, gridCell: TGridNode, hTree: HTree, hTable: HTable) = {

    //Recursive q to get all leaf nodes above this level
    val gridCells = hTree.findLeaves(gridCell)

    val gridGeoms = gridCells.map(_.geom).toArray
    val gridIds = gridCells.map(_.gridId).toArray

    val griddedEntities = executeIntersect(griddedEntity.geom, gridGeoms, gridIds, dimensionMask)

    //Make sure everything has an entity_id
    griddedEntities.foreach(_.entityId = griddedEntity.entityId)

    for (sg <- griddedEntities) {
      val keyOut = getRowKey(griddedEntity.entityId, sg.gridId)
      val put = GriddedObjectDAO.toPut(sg, keyOut)
      hTable.put(put)
      //More specialised classes can add extra columns
      //      addColumns(put, analysisUnit)
      //      buffer += put

    }
  }

  /**
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

        val sg = new GriddedObject

        sg.geom = result
        sg.gridId = gridIds(geomId)
        sgs.append(sg)
      }
      result = outGeoms.next
      geomId = outGeoms.getGeometryID
    }
    sgs.toList
  }


  def getRowKey(wdpaId: Long, gridId: Int): Array[Byte] = {
    Bytes.toBytes(wdpaId).reverse.++(Bytes.toBytes(gridId).reverse)
  }
}