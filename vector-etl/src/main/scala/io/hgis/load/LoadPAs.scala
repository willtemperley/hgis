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
object LoadPAs extends ObjectProvider[Site] {

  val em = DataAccess.em

  val clazz = classOf[Site]

  def main(args: Array[String]) {

    val hTable = new HTable(ConfigurationFactory.get, "pa_grid")
    execute(hTable)

  }

  override def getIds: Iterable[Any] = {
    //    val worst = "select site_id from hgrid.worst_100"
    val all = "SELECT id FROM protected_sites.wdpa_latest_all where is_designated"
    val q = em.createNativeQuery(all)
    q.getResultList
  }

}
