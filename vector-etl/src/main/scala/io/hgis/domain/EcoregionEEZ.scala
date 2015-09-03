package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.geom.MultiPolygon
import org.hibernate.annotations.Type

/**
 *
 * Created by tempehu on 01-Dec-14.
 */
@Entity
@Table(schema = "habitats_and_biotopes", name = "ecoregion_eez")
class EcoregionEEZ {

  @Id
  @Column(name = "id")
  var id: Int = _

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: MultiPolygon = _

  @Transient
  var geom: Geometry = _


}
