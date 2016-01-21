package io.hgis

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util

import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.GridNode
import io.hgis.domain.rl.TestSpecies
import io.hgis.hdomain.ConvertsGeometry
import io.hgis.load.{SpeciesGrid, DataAccess}
import io.hgis.op.HTree
import io.hgis.scanutil.TableIterator
import io.hgis.vector.domain.{GridNodeImpl, TGridNode}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._

/**
  *
  * Created by willtemperley@gmail.com on 14-Oct-15.
  */
class TestLoadSppContextSensitive extends TableIterator with ConvertsGeometry{

  val hTable = MockHTable.create()
  val CF = "cfv".getBytes

  val em = DataAccess.em

  val gridCells: Array[TGridNode] = {
    val q = em.createNativeQuery(
      s"""
        SELECT id, geom, is_leaf, parent_id
        from hgrid.h_grid2
        order by id
        """, classOf[GridNode])
    val gridCells = q.getResultList.map(_.asInstanceOf[GridNode])
    gridCells.foreach(f => f.geom = jtsToEsri(f.jtsGeom))
    gridCells.map(_.asInstanceOf[TGridNode]).toArray
  }

  val hTree: HTree = new HTree(gridCells, gridCells(0))

//  val spLoader = new LoadSppContextSensitive(em.find(classOf[TestSpecies], 10l), hTree, hTable)

  @Test
  def serializedHTree(): Unit = {

    val os = new ObjectOutputStream(new FileOutputStream("target/gridnodes.dat"))

    def copyGridNode(gridNode: TGridNode): GridNodeImpl = {
      val gn = new GridNodeImpl
      gn.geom = gridNode.geom
      gn.isLeaf = gridNode.isLeaf
      gn.parentId = gridNode.parentId
      gn.gridId = gridNode.gridId
      gn
    }

    val serialGridNodes = gridCells.map(copyGridNode)

    os.writeObject(serialGridNodes)
    os.close()

    val is = new ObjectInputStream(new FileInputStream("target/gridnodes.dat"))
    val obj = is.readObject().asInstanceOf[Array[TGridNode]]

    Assert.assertTrue(gridCells.length == obj.length)
    obj.foreach(f => Assert.assertTrue(f.getClass == classOf[GridNodeImpl]))

    is.close()

  }

  @Test
  def spark(): Unit = {

    val conf = new SparkConf().setMaster("local[8]").setAppName("test-local")
    val sc = new SparkContext(conf)

//    val loader = new SpeciesGrid(gridCells)
//
//    val spp: util.List[TestSpecies] = em.createQuery("from TestSpecies", classOf[TestSpecies]).getResultList
//    spp.foreach(f => f.geom = jtsToEsri(f.jtsGeom))
//
//    loader.execute(sc, spp)
  }

  @Test
  def go(): Unit = {

//    spLoader.executeLoad2()

    val scan = new Scan

    scan.addFamily(CF)
    val scanner = hTable.getScanner(scan)
    val iterator = getIterator(scanner)
    iterator.foreach(verify)

  }


  def verify(res: Result): Unit = {

    def ogcFidCol = AccessUtil.longColumn(CF, "ogc_fid") _
    def getEntityId = AccessUtil.longColumn(CF, "entity_id") _
    def getCatId = AccessUtil.intColumn(CF, "cat_id") _
    def getGeom = AccessUtil.geomColumn(new WKBReader, CF, "geom") _

//    val ogcFid = ogcFidCol(res)
    val entityId = getEntityId(res)
    println(entityId)
    val geomType = getGeom(res).getGeometryType

//    Assert.assertTrue(spLoader.getIds.contains(ogcFid))

    Assert.assertTrue(geomType.equals("Polygon") || geomType.equals("MultiPolygon"))

  }
}
