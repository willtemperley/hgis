package io.hgis.vector.domain

import io.hgis.hdomain.HasRowKey

/**
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TSiteOverlap extends HasRowKey {

  var siteId1: Int
  var siteId2: Int
  var area: Double


  override def getRowKey: Array[Byte] = {
    getRandomByteArray
  }

}
