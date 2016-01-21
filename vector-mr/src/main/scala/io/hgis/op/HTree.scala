package io.hgis.op

import java.io.{ObjectInputStream, InputStream}

import com.esri.core.geometry.{Envelope, SpatialReference, OperatorContains, Geometry}
import io.hgis.vector.domain.TGridNode

import scala.collection.mutable.ListBuffer

/**
  * This creates an ingoing tree of GridCells
  *
  * Created by willtemperley@gmail.com on 20-Nov-15.
  */
object HTree {

  def apply(inputStream: InputStream): HTree = {
    val objectInputStream = new ObjectInputStream(inputStream)
    val obj = objectInputStream.readObject()
    val gridCells = obj.asInstanceOf[Array[TGridNode]]
    val hTree = new HTree(gridCells, gridCells(0))
    objectInputStream.close()
    hTree
  }

}

class HTree(gridCells: Array[TGridNode], root: TGridNode) {

//  gridCells.foreach(f => if (f.geom == null) f.geom = jtsToEsri(f.jtsGeom))
  val sr = SpatialReference.create(4326)

  //Put the parent ids and indexes together
  val parentId2idx = gridCells.zipWithIndex.map(f => f._1.parentId -> f._2)

  val contains = OperatorContains.local()

  /*
  This maps the parents (using parentId) to the array indexes of the their child nodes
  This means O(1) access to the child nodes once their indexes are retrieved
   */
  val parentIdToList = parentId2idx.groupBy(_._1).map{case(parentId, array) => (parentId, array.map(_._2))}

  def getChildNodes(parentId: Int): Array[TGridNode] = {
    val x = parentIdToList.get(parentId)
    x.get.map(gridCells)
  }

  //Search for
  def findSmallestContainingNode(geom: Geometry): TGridNode = {

    val env = new Envelope
    geom.queryEnvelope(env)

    //Need to find out if any of the child nodes contain the env.  If so, this one overrides all the others
    var searchNode = root
    while(true) {
      val children = getChildNodes(searchNode.gridId)

      searchNode = children.find(f => contains.execute(f.geom, env, sr, null)).getOrElse(return searchNode)
      if (searchNode.isLeaf) return searchNode
    }
    root
  }

  def findLeaves(parentNode: TGridNode, listBuffer: ListBuffer[TGridNode] = new ListBuffer[TGridNode]): ListBuffer[TGridNode] = {

    if (parentNode.isLeaf) {
      listBuffer += parentNode
      return listBuffer
    }

    val nodeArrayIndexes = parentIdToList.get(parentNode.gridId).get
    for (idx <- nodeArrayIndexes) {
      val nextLevelGridCell = gridCells(idx)
      findLeaves(nextLevelGridCell, listBuffer)
    }
    listBuffer
  }
}
