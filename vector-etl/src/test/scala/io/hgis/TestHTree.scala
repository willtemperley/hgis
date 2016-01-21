package io.hgis

import com.vividsolutions.jts.geom.Geometry
import io.hgis.domain.GridNode
import io.hgis.domain.rl.Species
import io.hgis.hdomain.ConvertsGeometry
import io.hgis.load.DataAccess
import io.hgis.op.HTree
import io.hgis.vector.domain.TGridNode
import org.junit.{Assert, Test}

import scala.collection.mutable.ListBuffer

import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 23-Nov-15.
  */
class TestHTree extends ConvertsGeometry {

  val em = DataAccess.em

//  val expectedLeafCount = 330416
  val expectedLeafCount = 24794

  val treeBase = em.find(classOf[GridNode], 0)

  val hTree: HTree = {

    val q = em.createNativeQuery(
      s"""
        SELECT id, geom, is_leaf, parent_id
        from hgrid.h_grid2
        order by id
        """, classOf[GridNode])

    val gridCells = q.getResultList.map(_.asInstanceOf[GridNode])
    gridCells.foreach(f => f.geom = jtsToEsri(f.jtsGeom))
    val gridNodes = gridCells.map(_.asInstanceOf[TGridNode]).toArray
    new HTree(gridNodes, gridNodes(0))
  }

  @Test
  def leafNodes(): Unit = {

    val listBuffer = new ListBuffer[TGridNode]

    time {
      hTree.findLeaves(treeBase, listBuffer)
    }

    Assert.assertTrue(listBuffer.size.equals(expectedLeafCount))
    println(listBuffer.size)
  }

  @Test
  def childNodes(): Unit = {
    time {
      val children = hTree.getChildNodes(0)
      Assert.assertTrue(children.length.equals(2))
    }
  }

  @Test
  def testFindTreeNodesByEnv(): Unit = {

    val japaneseDormouse = em.find(classOf[Species], 9233l)

    japaneseDormouse.geom = jtsToEsri(japaneseDormouse.jtsGeom)
    val envelope = japaneseDormouse.geom

    time {
      val foundByCode = hTree.findSmallestContainingNode(envelope)
    }


//    Assert.assertTrue(foundByCode.gridId.equals(foundByDB.gridId))

  }


  def getTreeBase(geometry: Geometry): GridNode = {

    val env = geometry.getEnvelopeInternal
    val sql =
      """
      SELECT id from hgrid.h_grid_node
      where st_contains(geom,
      st_setsrid(st_makebox2d(st_makepoint(:minX, :minY), st_makepoint(:maxX, :maxY)), 4326)
      )
      and st_srid(geom) = 4326
      order by st_area(geom) asc
      limit 1;
      """
    val q = em.createNativeQuery(sql)
      .setParameter("minX", env.getMinX)
      .setParameter("minY", env.getMinY)
      .setParameter("maxX", env.getMaxX)
      .setParameter("maxY", env.getMaxY)

    val id = q.getFirstResult
    em.find(classOf[GridNode], id.asInstanceOf[Int])
  }


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    result
  }

}
