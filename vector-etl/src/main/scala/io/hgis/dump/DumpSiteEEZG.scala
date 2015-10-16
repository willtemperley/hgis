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
import io.hgis.domain.SiteEcoregionEEZ
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client._

object DumpSiteEEZG {

  val injector = Guice.createInjector(new JPAModule)
  val em = injector.getInstance(classOf[EntityManager])
  val COLFAM: Array[Byte] = SiteGridDAO.getCF

  val wkbReader = new WKBReader
  val wkbExportOp = OperatorExportToWkb.local()
  def main(args: Array[String]) {

    //    Preconditions.checkArgument(args.length == 3, "Table, CF and rowkey please!")

    val htable: HTableInterface = new HTable(ConfigurationFactory.get, "site_ecoregion_eez_g")
    System.out.println("Processing table " + htable)

    val scan: Scan = new Scan

//    scan.addFamily(COLFAM)
    scan.addColumn("cfv".getBytes, "ee_id".getBytes)
    scan.addColumn("cfv".getBytes, "site_id".getBytes)

    em.getTransaction.begin()
    var i = 0
    val scanner: ResultScanner = htable.getScanner(scan)
    var result: Result = scanner.next
    while (result != null) {
      i += 1
      persistEntity(result)
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

  def eeId =   AccessUtil.intColumn("cfv", "ee_id") _
  def siteId = AccessUtil.intColumn("cfv", "site_id") _

  def persistEntity(res: Result): Unit = {

    val x = new SiteEcoregionEEZ
//    x.jtsGeom = wkbReader.read(res.getValue("cfv".getBytes, "geom".getBytes))
    x.siteId = siteId(res)
    x.entityId = eeId(res)

    em.persist(x)
  }
}
