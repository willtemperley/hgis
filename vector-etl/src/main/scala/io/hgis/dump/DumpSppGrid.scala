package io.hgis.dump

/**
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import io.hgis.domain.SiteGrid
import io.hgis.domain.rl.SpeciesGrid
import io.hgis.hdomain.GriddedObjectDAO
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client._

class DumpSppGrid extends ExtractionBase  {

  val CF: Array[Byte] = "cfv".getBytes

//  def gridId = AccessUtil.intColumn(CF, "grid_id") _
//  def analysisUnitId = AccessUtil.longColumn(CF, "entity_id") _
//  def catId = AccessUtil.intColumn(CF, "cat_id") _

  def getScan: Scan = {
    val scan = new Scan
    scan.addFamily(CF)
  }

  var i = 0

  override def buildEntity(res: Result): Unit = {

    i+=1

    val sg = GriddedObjectDAO.fromResult(res, new SpeciesGrid)
    sg.jtsGeom = jtsWkbReader.read(res.getValue(CF, "geom".getBytes))
//    x.gridId = gridId(res)

    sg.entityId = i

    em.persist(sg)

  }


}

