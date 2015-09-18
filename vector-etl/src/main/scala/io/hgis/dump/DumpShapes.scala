package io.hgis.dump

/**
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import javax.persistence.EntityManager

import com.esri.core.geometry.{OperatorExportToWkb, WkbExportFlags}
import com.google.inject.Guice
import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.domain.SiteGrid
import io.hgis.inject.JPAModule
import io.hgis.scanutil.TableIterator
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client._

object DumpShapes extends TableIterator {

  val injector = Guice.createInjector(new JPAModule)
  val em = injector.getInstance(classOf[EntityManager])
  val COLFAM: Array[Byte] = SiteGridDAO.getCF

  val wkbReader = new WKBReader
  val wkbExportOp = OperatorExportToWkb.local()

  def main(args: Array[String]) {

    //    Preconditions.checkArgument(args.length == 3, "Table, CF and rowkey please!")

    val htable: HTableInterface = new HTable(ConfigurationFactory.get, "site_grid")
    System.out.println("Got Htable " + htable)

    val scan: Scan = new Scan

    scan.addFamily(COLFAM)

    em.getTransaction.begin()
    var i = 0
    val scanner: ResultScanner = htable.getScanner(scan)

    val results = getIterator(scanner)

    for (result <- results) {

      i += 1
      writeSiteGrid(result)
      if (i % 1000 == 0) {
        println(i)
        em.getTransaction.commit()
        em.getTransaction.begin()
      }

    }
    em.getTransaction.commit()

    htable.close()
  }

  def writeSiteGrid(res: Result): Unit = {

    val x = new SiteGrid
    SiteGridDAO.fromResult(res, x)

    val ixPaGrid: Array[Byte] = wkbExportOp.execute(WkbExportFlags.wkbExportDefaults, x.geom, null).array()
    x.jtsGeom = wkbReader.read(ixPaGrid)

    em.persist(x)

  }
}

