package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.AnalysisUnit
import org.hibernate.annotations.Type

/**
 *
 * Created by tempehu on 01-Dec-14.
 */
@Entity
@Table(schema = "habitats_and_biotopes", name = "ecoregion_eez")
class EcoregionEEZ extends AnalysisUnit {

  @Id
    @Column(name = "id")
    var entityId: Long = _

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

  @Transient
  var geom: Geometry = _

}
