package io.hgis.load

import javax.persistence.EntityManager

import com.esri.core.geometry.Geometry
import io.hgis.ConfigurationFactory
import io.hgis.domain.{AdminUnit, GridCell}
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTable

import java.util.ArrayList

import scala.collection.JavaConversions._

/**
 * It's not obvious how the ESRI geometry API preserves ids of intersectors
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
object DirectLoadAdminUnits extends ObjectProvider[AdminUnit] {

  val em = DataAccess.em

  val clazz = classOf[AdminUnit]

  override def getIds: Iterable[Any] = {
    val q = em.createQuery("select id from AdminUnit")
    q.getResultList
  }

  def main(args: Array[String]) {

    val hTable = new HTable(ConfigurationFactory.get, "country_grid")
    execute(hTable)

  }


}
