package io.hgis.vector.domain

import io.hgis.hdomain.{AnalysisUnit, HasRowKey}

/**
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TSiteOverlap extends HasRowKey with AnalysisUnit {

  var siteId1: Long
  var siteId2: Long
  var area: Double


  override def getRowKey: Array[Byte] = {
    getRandomByteArray
  }

}
