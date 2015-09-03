package io.hgis.dump

/**
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import javax.persistence.EntityManager

import com.esri.core.geometry.OperatorExportToWkb
import com.google.inject.Guice
import com.vividsolutions.jts.io.WKBReader
import io.hgis.domain.SiteOverlap
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteOverlapDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._

object DumpOverlaps {

  val injector = Guice.createInjector(new JPAModule)
  val conf = injector.getInstance(classOf[Configuration])
  val em = injector.getInstance(classOf[EntityManager])

  val wkbReader = new WKBReader
  val wkbExportOp = OperatorExportToWkb.local()
  def main(args: Array[String]) {

//    Preconditions.checkArgument(args.length == 3, "Table, CF and rowkey please!")

    val htable: HTableInterface = new HTable(conf, "site_overlap")
    System.out.println("Got Htable " + htable)

    val scan: Scan = new Scan

    scan.addFamily(SiteOverlapDAO.getCF)

    em.getTransaction.begin()
    var i = 0
    val scanner: ResultScanner = htable.getScanner(scan)
      var result: Result = scanner.next
      while (result != null) {
        i += 1
        writeSiteGrid(result)
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

  def writeSiteGrid(res: Result): Unit = {

    val x = new SiteOverlap
    SiteOverlapDAO.fromResult(res, x)
    em.persist(x)

  }
}

