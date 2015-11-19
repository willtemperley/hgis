package io.hgis.load

import io.hgis.domain.{LoadQueue, Site}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT

import scala.collection.JavaConversions._

/**
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
class DirectLoadPAs extends GridLoader[Site](classOf[Site]) {

  override def getIds: Iterable[Any] = {
//    val all = "SELECT id FROM protected_sites.wdpa_latest_all where is_designated order by 1"
    val all =
    """
    SELECT id FROM protected_sites.wdpa_latest_all where id not in
    (select site_id from hgrid.pa_load_log where is_loaded)
    order by id
    limit 25000
    """

    val res = em.createNativeQuery(all)
    val ret = res.getResultList.map(f => f.asInstanceOf[Int].toLong)

    println("Number of sites to be processed: " + ret.length)
    ret
  }

  override def notifyComplete(obj: Site): Unit = {
//    val paLoaded = new LoadQueue
//    paLoaded.isLoaded = true
//
//    //FIXME
//    paLoaded.id = new paLoaded.LqId
//    paLoaded.id.entityId = obj.entityId
//    paLoaded.id.entityType = "pa"
////    paLoaded.id
////      = obj.entityId
//
//    em.getTransaction.begin()
//    em.persist(paLoaded)
//    em.getTransaction.commit()
  }


  override def addColumns(put: Put, obj: Site): Unit = {
    put.add(GridLoader.CF, "cat_id".getBytes, Bytes.toBytes(obj.iucnCategory.catId))
    put.add(GridLoader.CF, "is_point".getBytes, Bytes.toBytes(obj.isPoint))
    put.add(GridLoader.CF, "is_designated".getBytes, Bytes.toBytes(obj.isDesignated))
  }
}

