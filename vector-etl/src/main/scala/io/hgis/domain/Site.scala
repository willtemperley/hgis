package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.geom.MultiPolygon
import io.hgis.hdomain.{AnalysisUnit, HasRowKey}
import io.hgis.vector.domain.TSite
import org.apache.hadoop.hbase.util.Bytes
import org.hibernate.annotations.Type

/**
 *
 * Created by will on 24/10/2014.
 */
@Entity
@Table(schema = "protected_sites", name = "wdpa_latest_all")
class Site extends HasRowKey with TSite with AnalysisUnit {

  @Id
  @Column(name = "id")
  override var entityId: Long = _

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

  @ManyToOne
  @JoinColumn(name = "iucn_cat")
  var iucnCategory: IUCNCat = _

  @Transient
  override var catId: Int = _

  @Column(name = "is_designated")
  override var isDesignated: Boolean = _

  @Transient
  override var geom: Geometry = _

  @Transient
  override var gridCells: Array[String] = _

  @Transient
  override var gridIdList: Array[String] = _

  @Transient
  override def getRowKey: Array[Byte] = {
    getRandomByteArray ++ Bytes.toBytes(entityId)
  }

  @Column(name = "is_point")
  override var isPoint: Boolean = _

}