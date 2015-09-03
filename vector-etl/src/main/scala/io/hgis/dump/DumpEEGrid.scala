package io.hgis.dump

/**
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import javax.persistence.EntityManager

import com.esri.core.geometry.OperatorExportToWkb
import com.google.inject.Guice
import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.EEGrid
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client._

object DumpEEGrid {

  val injector = Guice.createInjector(new JPAModule)
  val em = injector.getInstance(classOf[EntityManager])
  val COLFAM: Array[Byte] = SiteGridDAO.getCF

  val wkbReader = new WKBReader
  val wkbExportOp = OperatorExportToWkb.local()
  def main(args: Array[String]) {

//    Preconditions.checkArgument(args.length == 3, "Table, CF and rowkey please!")

    val htable: HTableInterface = new HTable(ConfigurationFactory.get, "ee_grid")
    System.out.println("Processing table " + htable)

    val scan: Scan = new Scan

    scan.addFamily(COLFAM)

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

  def gridId = AccessUtil.intColumn("cfv", "grid_id") _

  //FIXME should be ee_id
  def eeId = AccessUtil.intColumn("cfv", "site_id") _

  def persistEntity(res: Result): Unit = {

    val x = new EEGrid

    x.geom = wkbReader.read(res.getValue("cfv".getBytes, "geom".getBytes))
    x.gridId = gridId(res)
    x.eeId = eeId(res)

    em.persist(x)

  }
}

