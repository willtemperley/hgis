package io.hgis.hdomain

import scala.util.Random

/**
 * Some basic operations many domain objects will require
 *
 * Created by willtemperley@gmail.com on 17-Nov-14.
 */
trait HasRowKey {

  def getRowKey: Array[Byte]

  /**
   * Generates a random byte array which is useful for creating uniform but random spreads across HBase regions
   *
   * @return
   */
  def getRandomByteArray: Array[Byte] = {

    val nextBytes = new Array[Byte](8)
    Random.nextBytes(nextBytes)
    nextBytes
  }
}
