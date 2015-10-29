package io.hgis.load

import io.hgis.domain.EcoregionEEZ
import org.apache.hadoop.hbase.client.Put

import scala.collection.JavaConversions._

/**
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
class LoadEEZs extends GridLoader[EcoregionEEZ](classOf[EcoregionEEZ]) {

  override def getIds: Iterable[Any] = {
    val q = em.createQuery("select id from EcoregionEEZ")
    q.getResultList
  }

  override def addColumns(put: Put, obj: EcoregionEEZ): Unit = {

  }
}
