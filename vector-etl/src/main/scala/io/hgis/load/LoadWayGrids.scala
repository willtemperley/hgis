package io.hgis.load

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.GridCell
import io.hgis.domain.osm.Way
import io.hgis.hdomain.GriddedObjectDAO
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{NullComparator, BinaryComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Takes ways from hbase and populates another table with the gridded version, taking grid info from postgres.
 *
 * Overrides normal behaviour hence some no-ops
 *
 * Created by willtemperley@gmail.com on 20-Oct-15.
 */
class LoadWayGrids extends GridLoader[Way](classOf[Way], Geometry.Type.Polyline) {

  val geomColumn  = AccessUtil.geomColumn(new WKBReader(), "cfv", "geom") _
  val speedColumn  = AccessUtil.stringColumn("cft", "maxspeed") _
//  val idColumn  = AccessUtil.stringColumn("cfv", "id") _
//  val highwayColumn  = AccessUtil.stringColumn("cft", "highway") _
//  val railwayColumn  = AccessUtil.stringColumn("cft", "railway") _
//  val wayIdColumn  = AccessUtil.stringColumn("cft", "id") _


  val CFV: Array[Byte] = "cfv".getBytes
  val CFT: Array[Byte] = "cft".getBytes

  val HIGHWAY = "highway".getBytes
  val RAILWAY = "railway".getBytes
  val SPEED = "maxspeed".getBytes
  val ID = "id".getBytes

  override def executeLoad(table: HTableInterface) {

    val s2 = new Scan()
    println("dimension mask is " + dimensionMask)

    s2.addFamily(CFV)
    s2.addFamily(CFT)

    //    s2.addColumn("cfv".getBytes, "id".getBytes)
    //    s2.addColumn("cfv".getBytes, "geom".getBytes)
    //
    //    s2.addColumn("cft".getBytes, "highway".getBytes)
    //    s2.addColumn("cft".getBytes, "railway".getBytes)
    //    s2.addColumn("cft".getBytes, "maxspeed".getBytes)
    //    s2.setFilter(new SingleColumnValueFilter("cfv".getBytes, "geom".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null)))
    val filter = new SingleColumnValueFilter(CFT, "maxspeed".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null))
    filter.setFilterIfMissing(true)
    s2.setFilter(filter)

    val hT = new HTable(ConfigurationFactory.get, "transport")

    val scanner = hT.getScanner(s2)
    val itr = Iterator.continually(scanner.next).takeWhile(_ != null)

//    val wayObj = new Way

    var i = 0
    var nGrids = 0
    for (hWay: Result <- itr) {
      val geom = geomColumn(hWay)


//      val analysisUnit = new AnalysisUnitX
//      wayObj.geom = jtsToEsri(geom, esriGeomType = geomType)
//      wayObj.entityId = 0//idColumn(hWay).toLong
//      wayObj.railwayTag = railwayColumn(hWay)
//      wayObj.highwayTag = highwayColumn(hWay)
//      wayObj.speedTag = speedColumn(hWay)

//      println(
//      hWay.toString
//      )

//      println("geom " + wayObj.geom)
//      println("highway " + wayObj.highwayTag)
//      println("rail " + wayObj.railwayTag)
//      println("speed " + wayObj.speedTag)


      if (speedColumn(hWay) != null) {
        val grids = getHGrid(geom.getEnvelopeInternal)

        for (g <- grids) {

          val ix = g.jtsGeom.intersection(geom)

          if (ix.getLength > 0) {
            nGrids +=1
            val wkb = jtsWkbWriter.write(ix)

            val bytes = new Array[Byte](8)
            Random.nextBytes(bytes)
            val put = new Put(bytes)

            put.add(CFV, GEOM, wkb)
            put.add(CFV, GRID_ID, Bytes.toBytes(g.gridId))

            //UNTESTED
            def transferCell = AccessUtil.transferCell(hWay, put, CFT) _
            Array(ID,HIGHWAY,RAILWAY,SPEED).foreach(transferCell)

//            put.add(CFT, ID, hWay.getValue(CFT, ID))
//            put.add(CFT, HIGHWAY, hWay.getValue(CFT, HIGHWAY))
//            put.add(CFT, RAILWAY, hWay.getValue(CFT, RAILWAY))
//            put.add(CFT, SPEED, hWay.getValue(CFT, SPEED))

            table.put(put)

          }

        }

//        nGrids += insertGridCells(table, wayObj)

        i += 1
        if (i % 5000 == 0) {
          println(i)
          println(nGrids + " grids inserted so far")
          table.flushCommits()
        }

      }
    }
    table.flushCommits()
    table.close()
  }

  /*
  Subclasses can override this to specialise their hbase rows
   */
  override def addColumns(put: Put, obj: Way): Unit = {
    if (obj.highwayTag != null) put.add(CFT, HIGHWAY, Bytes.toBytes(obj.highwayTag))
    if (obj.railwayTag != null) put.add(CFT, RAILWAY, Bytes.toBytes(obj.railwayTag))
    if (obj.speedTag != null) put.add(CFT, SPEED, Bytes.toBytes(obj.speedTag))

  }

  override def getIds: Iterable[Any] = {
    null
  }

}
