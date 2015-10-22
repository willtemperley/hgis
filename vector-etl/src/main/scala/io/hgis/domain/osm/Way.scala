package io.hgis.domain.osm

import javax.persistence._

import com.esri.core.geometry
import com.vividsolutions.jts.geom.Geometry
import io.hgis.osmdomain.TWay
import org.hibernate.annotations.Type

/**
 *
 * Created by tempehu on 01-Dec-14.
 */
@Entity
@Table(schema = "public", name = "ways")
class Way extends TWay {

  @Transient
  var geom: com.esri.core.geometry.Geometry = _

  @Id
  @Column(name = "id")
  override var entityId: Long = _

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name="linestring")
  override var jtsGeom: Geometry = _

  @Transient
  var speedTag: String = _

  @Transient
  var highwayTag: String = _

  @Transient
  var railwayTag: String = _

}
