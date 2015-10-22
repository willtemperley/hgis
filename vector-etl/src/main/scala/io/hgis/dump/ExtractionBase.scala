package io.hgis.dump

import io.hgis.hdomain.ConvertsGeometry
import io.hgis.load.DataAccess
import org.apache.hadoop.hbase.client.{HTable, Result, Scan}

/**
 *
 * Created by willtemperley@gmail.com on 22-Oct-15.
 */
abstract class ExtractionBase[T] extends ConvertsGeometry {

  val em = DataAccess.em

  def getScan: Scan

  def createEntity: T

  def persistEntity(result: Result, obj: T): Unit

  val TX_SIZE: Int = 1000

  def executeExtract(hTable: HTable): Unit = {

    val scan = getScan

    em.getTransaction.begin()
    var i = 0
    val scanner = hTable.getScanner(scan)
    var result = scanner.next
    while (result != null) {
      i += 1
      persistEntity(result, createEntity)
      if (i % TX_SIZE == 0) {
        println(i)
        em.getTransaction.commit()
        em.getTransaction.begin()
      }

      result = scanner.next
    }
    em.getTransaction.commit()
    hTable.close()
  }

}
