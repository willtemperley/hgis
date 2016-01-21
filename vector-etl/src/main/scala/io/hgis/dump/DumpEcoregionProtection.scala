package io.hgis.dump

/**
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import javax.persistence.EntityManager

import com.esri.core.geometry.OperatorExportToWkb
import com.google.inject.Guice
import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.{EEPro, EcoregionEEZProtection}
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes

class DumpEcoregionProtection extends ExtractionBase  {

  val COLFAM: Array[Byte] = "cfv".getBytes

  val injector = Guice.createInjector(new JPAModule)

  var catID: Int = _

  override def withArguments(args: Array[String]) = {
    catID = args(0).toInt
    println(s"extracting category $catID")
    this
  }

  def gridId = AccessUtil.intColumn(COLFAM, "grid_id") _
  def analysisUnitId = AccessUtil.longColumn(COLFAM, "entity_id") _
  def catIdCol = AccessUtil.intColumn(COLFAM, "cat_id") _

  def getScan: Scan = {
    val scan: Scan = new Scan
    scan.addFamily(COLFAM)
    val filter = new SingleColumnValueFilter(COLFAM, "cat_id".getBytes, CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(catID)))
    scan.setFilter(filter)
    scan
  }

  override def buildEntity(res: Result): Unit = {
    val x = new EcoregionEEZProtection
    x.jtsGeom = jtsWkbReader.read(res.getValue("cfv".getBytes, "geom".getBytes))
    x.gridId = gridId(res)
    x.entityId = analysisUnitId(res)
    x.catId = catIdCol(res)

    em.persist(x)

  }

}

