package io.hgis.load.osm

import javax.persistence.EntityManager

import com.esri.core.geometry.OperatorImportFromWkb
import com.google.inject.Guice
import com.vividsolutions.jts.io.{WKBWriter, WKTWriter}
import io.hgis.ConfigurationFactory
import io.hgis.domain.osm.Way
import io.hgis.inject.JPAModule
import io.hgis.osmdomain.WayDAO
import org.apache.hadoop.hbase.client.HTable

import scala.collection.JavaConversions._

/**
 * Re loads all site grids ...
 *
 * Created by will on 24/10/2014.
 */
object LoadWays {

  class TableSplit(val nRows: Int, interval: Int = 1000000)  {

    //FIXME unfinished
    val nP = Math.ceil(nRows.toDouble / interval).toInt
    def intervals = (0 until nP).map(f => (f * interval, f + interval))
    
  }

  val injector = Guice.createInjector(new JPAModule)
  val wktWriter = new WKTWriter
  val wkbWriter = new WKBWriter
  var wkbReader = OperatorImportFromWkb.local()

  def main(args: Array[String]) {

    val em = injector.getInstance(classOf[EntityManager])

    val hTable = new HTable(ConfigurationFactory.get, "ways")

    val q = em.createQuery("from Way", classOf[Way]).setMaxResults(1000)
    val results = q.getResultList

    var i = 0
    for (r <- results) {

      val put = WayDAO.toPut(r, r.getRowKey)
      hTable.put(put)

      if (i % 1000 == 0) {
        hTable.flushCommits()
        println(i)
      }
      i += 1
    }

    hTable.flushCommits()
    println("Loaded " + i + " ways.")
  }

}
