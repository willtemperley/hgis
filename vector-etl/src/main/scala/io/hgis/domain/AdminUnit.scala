package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.geom.MultiPolygon
import io.hgis.vector.domain.TAdminUnit
import org.hibernate.annotations.Type

/**
 * Maps the country table
 *
 * Created by tempehu on 01-Dec-14.
 */
@Entity
@Table(schema = "administrative_units", name = "country")
class AdminUnit extends TAdminUnit {


  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: MultiPolygon = _

  @Transient
  override var geom: Geometry = _

  @Column
  var name: String = _

  @Id
  @Column(name = "id")
  override var siteId: Int = _

  @Transient
  override var gridCells: Array[String] = _

  @Transient
  override var gridIdList: Array[String] = _
}
