package io.hgis.load

import com.esri.core.geometry.Geometry
import io.hgis.ConfigurationFactory
import io.hgis.domain.Site
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, HTableInterface, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

/**
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
class LoadPAs extends GridLoader[Site](classOf[Site]) {

  override def getIds: Iterable[Any] = {
    //    val worst = "select site_id from hgrid.worst_100"
    val all = "SELECT id FROM protected_sites.wdpa_latest_all where is_designated order by 1"
    em.createNativeQuery(all).getResultList

  }

  override def addColumns(put: Put, obj: Site): Unit = {
    put.add(GridLoader.CF, "cat_id".getBytes, Bytes.toBytes(obj.iucnCategory.catId))
  }
}

