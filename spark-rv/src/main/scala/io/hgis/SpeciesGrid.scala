package io.hgis


import com.esri.core.geometry._
import io.hgis.hdomain.GriddedObjectDAO.GriddedObject
import io.hgis.hdomain.{AnalysisUnitDAO, AnalysisUnit, GriddedEntity, GriddedObjectDAO}
import io.hgis.op.HTree
import io.hgis.vector.domain.TGridNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Result, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by willtemperley@gmail.com on 10-Dec-15.
  *
  * These class params are used so they can be mocked
  */
object SpeciesGrid extends Serializable {


  val pointCountThreshold = 12500
  val dimensionMask = 4
  val sr = SpatialReference.create(4326)

  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val tableName = "sp_test"
    println("Table " + tableName)

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cfv")

    val sparkConf = new SparkConf().setAppName("sp_grid_spark")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    def pWorker = partitionWorker(hTableName = "sp_grid") _

    rdd.foreachPartition(pWorker)

  }

  def partitionWorker(hTableName: String)(analysisUnits: Iterator[(ImmutableBytesWritable, Result)]): Unit = {

    val pt = new Path("hdfs:/user/tempehu/gridnodes.dat")
    val fs = FileSystem.get(new Configuration())

    val hTree = HTree(fs.open(pt))

    val hTable = new HTable(ConfigurationFactory.get, hTableName)

    def worker = objectWorker(hTree, hTable) _
    analysisUnits.foreach(f => worker(f._2))
  }

  def objectWorker(hTree: HTree, hTable: HTable)(result: Result): Unit = {

    val analysisUnit = AnalysisUnitDAO.fromResult(result)

    if (analysisUnit.entityId == 0) throw new RuntimeException("missing entity id")

    val startNode = hTree.findSmallestContainingNode(analysisUnit.geom)
    doGridding(startNode, analysisUnit, hTree, hTable)
  }

  def doGridding(gridCell: TGridNode, griddedEntity: AnalysisUnit, hTree: HTree, hTable: HTable ): Unit = {

//    if (griddedEntity.geom == null) {
//      griddedEntity.geom = jtsToEsri(griddedEntity.jtsGeom)
//    }

    val pointCount = griddedEntity.geom.asInstanceOf[Polygon].getPointCount

    //BASE CASE -- either there are too few points to bother continuing to recurse, or we've reached the leaf
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