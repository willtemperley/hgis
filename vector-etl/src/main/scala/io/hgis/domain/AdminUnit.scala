package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.geom.MultiPolygon
import io.hgis.hdomain.AnalysisUnit
import io.hgis.vector.domain.TAdminUnit
import org.hibernate.annotations.Type

/**
 * Maps the country table
 *
 * Created by tempehu on 01-Dec-14.
 */
@Entity
@Table(schema = "public", name = "admin0")
class AdminUnit extends TAdminUnit with AnalysisUnit {

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

  @Transient
  override var geom: Geometry = _

  @Column
  var name: String = _

  @Column(name = "country_id")
  var iso3: String = _

  @Id
  @Column(name = "id")
  override var entityId: Long = _


}
