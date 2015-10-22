package io.hgis

import io.hgis.hdomain.AnalysisUnit
import io.hgis.load.DataAccess
import io.hgis.vector.domain.TSiteGrid

/**
 *
 * Created by willtemperley@gmail.com on 21-Oct-15.
 */
abstract class SerializationTestBase[T <: AnalysisUnit] {

  val hTable = MockHTable.create()
  val em = DataAccess.em

  def serializeAndDeserialize(): Unit

  def areEqual(original: T, reconstituted: T): Boolean = {
    if (!original.geom.equals(reconstituted.geom)) return false
    if (!original.entityId.equals(reconstituted.entityId)) return false
    true
  }

}
