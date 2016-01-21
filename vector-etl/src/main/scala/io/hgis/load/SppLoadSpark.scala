package io.hgis.load

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util

import com.vividsolutions.jts.io.WKBReader
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.GridNode
import io.hgis.domain.rl.TestSpecies
import io.hgis.hdomain.ConvertsGeometry
import io.hgis.op.HTree
import io.hgis.scanutil.TableIterator
import io.hgis.vector.domain.TGridNode
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._

/**
  *
  * Created by willtemperley@gmail.com on 14-Oct-15.
  */
object SppLoadSpark extends ConvertsGeometry{

  val em = DataAccess.em

  val gridCells: Array[TGridNode] = {
    val q = em.createNativeQuery(
      s"""
        SELECT id, geom, is_leaf, parent_id
        from hgrid.h_grid2
        order by id
        """, classOf[GridNode])
    val gridCells = q.getResultList.map(_.asInstanceOf[TGridNode]).toArray
    gridCells
  }

  val hTree: HTree = new HTree(gridCells, gridCells(0))

  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("spp-loader")
    val sc = new SparkContext(conf)

    val loader = new SpeciesGrid(gridCells)

    val spp: util.List[TestSpecies] = em.createQuery("from TestSpecies", classOf[TestSpecies]).getResultList

    println("DB work done")

    spp.foreach(f => f.geom = jtsToEsri(f.jtsGeom))
    loader.execute(sc, spp)
  }
}
