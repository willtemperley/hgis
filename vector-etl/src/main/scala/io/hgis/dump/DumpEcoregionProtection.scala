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

object DumpEcoregionProtection {

  val injector = Guice.createInjector(new JPAModule)
  val em = injector.getInstance(classOf[EntityManager])
  val COLFAM: Array[Byte] = SiteGridDAO.getCF

  val wkbReader = new WKBReader
  val wkbExportOp = OperatorExportToWkb.local()

  def main(args: Array[String]) {

    val htable = new HTable(ConfigurationFactory.get, "ee_protection")
    System.out.println("Processing table " + htable)

    val scan: Scan = new Scan
    scan.addFamily(COLFAM)

    val filter = new SingleColumnValueFilter(COLFAM, "cat_id".getBytes, CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(args(1).toInt)))
    scan.setFilter(filter)

    em.getTransaction.begin()
    var i = 0
    val scanner = htable.getScanner(scan)
    var result = scanner.next
    while (result != null) {
      i += 1
      persistEntity(result, new EcoregionEEZProtection)
      if (i % 1000 == 0) {
        println(i)
        em.getTransaction.commit()
        em.getTransaction.begin()
      }

      result = scanner.next
    }
    em.getTransaction.commit()

    htable.close()
  }

  def gridId = AccessUtil.intColumn("cfv", "grid_id") _

  def analysisUnitId = AccessUtil.intColumn("cfv", "country_id") _

  def catId = AccessUtil.intColumn("cfv", "cat_id") _

  def persistEntity(res: Result, x: EEPro): Unit = {

    x.jtsGeom = wkbReader.read(res.getValue("cfv".getBytes, "geom".getBytes))
    x.gridId = gridId(res)
    x.analysisUnitId = analysisUnitId(res)
    x.catId = catId(res)

    em.persist(x)

  }
}

