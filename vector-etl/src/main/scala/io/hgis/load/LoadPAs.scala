package io.hgis.load

import io.hgis.domain.Site
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

/**
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
class LoadPAs extends GridLoader[Site](classOf[Site]) {

  override def getIds: Iterable[Any] = {
//    val all = "SELECT id FROM protected_sites.wdpa_latest_all where is_designated order by 1"
    val all = "SELECT id FROM protected_sites.wdpa_latest_all where wdpa_id not in (select wdpa_id from hgrid.worst_100) order by 1"
    val res = em.createNativeQuery(all)
    val ret = res.getResultList.map(f => f.asInstanceOf[Int].toLong)
    println("Number of sites to be processed: " + ret.length)
    ret
  }

  override def addColumns(put: Put, obj: Site): Unit = {
    put.add(GridLoader.CF, "cat_id".getBytes, Bytes.toBytes(obj.iucnCategory.catId))
  }
}

