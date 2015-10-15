package io.hgis.load

import com.esri.core.geometry.Geometry
import io.hgis.ConfigurationFactory
import io.hgis.domain.Site
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTableInterface, HTable}

import scala.collection.JavaConversions._

/**
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
object DirectLoadSite extends ObjectProvider[Site] {

  val em = DataAccess.em

  val clazz = classOf[Site]

  override def getIds: Iterable[Any] = {

    val q = em.createNativeQuery("select site_id from hgrid.worst_100").setMaxResults(2)// where wdpa_id = 20615")
    q.getResultList

  }
}
