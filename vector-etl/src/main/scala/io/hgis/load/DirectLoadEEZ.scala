package io.hgis.load

import com.esri.core.geometry.Geometry
import io.hgis.ConfigurationFactory
import io.hgis.domain.EcoregionEEZ
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTable

import scala.collection.JavaConversions._

/**
 * It's not obvious how the ESRI geometry API preserves ids of intersectors
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
object DirectLoadEEZ extends ObjectProvider[EcoregionEEZ] {

  val em = DataAccess.em
  val clazz = classOf[EcoregionEEZ]

  override def getIds: Iterable[Any] = {
    val q = em.createQuery("select id from EcoregionEEZ")
    q.getResultList
  }


  def main(args: Array[String]) {

    val hTable = new HTable(ConfigurationFactory.get, "ee_grid")
    execute(hTable)

  }

}
