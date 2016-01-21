package io.hgis.vector.mr

/**
 * Intersects PAs and their grids
 *
 * Copies all output to another HBase table
 *
 * Required some hacking to work:
 * https://github.com/ndimiduk/hbase-fatjar
 *
 * Magic incantation:
 * HADOOP_CLASSPATH=$(hbase classpath) hadoop jar target/hbase-mr-0.1-SNAPSHOT-jar-with-dependencies.jar io.hgis.mr.GridLoaderMR <src> <dest>
 *
 * Created by willtemperley@gmail.com on 17-Nov-14.
 */

import java.io.{FileInputStream, ObjectInputStream, BufferedReader, InputStreamReader}

import com.esri.core.geometry._
import com.vividsolutions.jts
import io.hgis.hdomain.GriddedObjectDAO.GriddedObject
import io.hgis.hdomain.{AnalysisUnit, AnalysisUnitDAO, GriddedEntity, GriddedObjectDAO}
import io.hgis.op.{HTree, IntersectUtil}
import io.hgis.osmdomain.WayDAO
import io.hgis.vector.domain.{TGridNode, TGridCell, SiteDAO, SiteGridDAO}
import org.apache.directory.api.util.ByteBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapper, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper}

import scala.collection.mutable.ListBuffer

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object SpeciesGridLoaderMR {

  def main(args: Array[String]) {

    val conf: Configuration =  new Configuration
    //    conf.set(CF_RAS_SCAN, colFam)
    //    conf.set(TABLE_NAME, tableName)
    if (args.length != 2) throw new RuntimeException("Please specify an input and output table.")

    val inTable = args(0)
    val outTable = args(1)

    val scan = new Scan
    scan.addFamily(SiteGridDAO.getCF)
    val filter = new SingleColumnValueFilter(SiteGridDAO.getCF, "family".getBytes, CompareOp.EQUAL, new BinaryComparator("DELPHINIDAE".getBytes))
    scan.setFilter(filter)

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    TableMapReduceUtil.initTableMapperJob(inTable, scan, classOf[SpeciesGridLoaderMR.GridIXMapper], classOf[ImmutableBytesWritable], classOf[Put], job)
    TableMapReduceUtil.addDependencyJars(job)

    /* No reducer required */
    TableMapReduceUtil.initTableReducerJob(outTable, null, job)
    job.waitForCompletion(true)

  }

  class GridIXMapper extends TableMapper[ImmutableBytesWritable, Put] {

    val wktImportOp = OperatorImportFromWkt.local()
    val outputKey = new ImmutableBytesWritable

//    var gridCells: Array[TGridNode] = _
    var hTree: HTree = _
    val sr = SpatialReference.create(4326)
    val pointCountThreshold: Int = 12500
    val dimensionMask = 4

    override def setup(context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      val pt = new Path("hdfs:/user/tempehu/gridcells.dat")
      val fs = FileSystem.get(new Configuration())
      hTree = HTree(fs.open(pt))

      context.progress()
    }


    def doGridding(gridCell: TGridNode, griddedEntity: AnalysisUnit, context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {


      val pointCount = griddedEntity.geom.asInstanceOf[Polygon].getPointCount

      //BASE CASE
      if (pointCount <= pointCountThreshold || gridCell.isLeaf) {

        executeStage2(griddedEntity, gridCell, context)
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

          doGridding(gridCells(geomId), sg, context)
          context.setStatus("gridCell: " + gridCell.gridId)
          context.progress()

        }
        result = outGeoms.next
        geomId = outGeoms.getGeometryID
      }
    }

    def executeStage2(griddedEntity: AnalysisUnit, gridCell: TGridNode, context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context) = {

      //Recursive q to get all leaf nodes above this level
      val gridCells = hTree.findLeaves(gridCell)

      val gridGeoms = gridCells.map(_.geom).toArray
      val gridIds = gridCells.map(_.gridId).toArray

      val griddedEntities = executeIntersect(griddedEntity.geom, gridGeoms, gridIds, dimensionMask, context)

      //Make sure everything has an entity_id
      griddedEntities.foreach(_.entityId = griddedEntity.entityId)

      for (sg <- griddedEntities) {
        val keyOut = getRowKey(griddedEntity.entityId, sg.gridId)
        val put = GriddedObjectDAO.toPut(sg, keyOut)
        //More specialised classes can add extra columns
        //      addColumns(put, analysisUnit)
        //      buffer += put
        outputKey.set(keyOut)
        context.write(outputKey, put)

      }
    }

    /**
      *
      * @param geomList the intersectors (e.g. a grid)
      * @param gridIds the ids of the intersectors
      * @return
      */
    def executeIntersect(geom: com.esri.core.geometry.Geometry, geomList: Array[com.esri.core.geometry.Geometry], gridIds: Array[Int], dimensionMask: Int, context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): List[GriddedEntity] = {

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
          context.progress()
          context.setStatus("working")

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

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      val au = new AnalysisUnit {
        override var entityId: Long = _
        override var geom: Geometry = _
        override var jtsGeom: jts.geom.Geometry = _
      }

      AnalysisUnitDAO.fromResult(result, au)
      //FIXME
//      val gridCell = new TGridCell {override var geom: Geometry = _
//        override var gridId: Int = _

//      val baseNode = hTree.findSmallestContainingNode(au.jtsGeom)

//      doGridding(baseNode, au, context)

    }
  }

}
