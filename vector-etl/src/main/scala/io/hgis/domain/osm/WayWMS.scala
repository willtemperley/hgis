package io.hgis.domain.osm

import javax.persistence._

import com.vividsolutions.jts.geom.Geometry
import io.hgis.osmdomain.TWay
import org.hibernate.annotations.Type

/**
 * Maps the country table
 *
 * Created by tempehu on 01-Dec-14.
 */
@Entity
@Table(schema = "public", name = "way_wms")
class WayWMS extends TWay {

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column
  var linestring: Geometry = _

  @Column
  var ref: String = _

  @Column
  var highway: String = _

  @Column
  var name: String = _

  @Column
  var amenity: String = _

  @Column
  var tunnel: String = _

  @Column
  var railway: String = _

  @Id
  @Column(name = "id")
  var id: Int = _

}
