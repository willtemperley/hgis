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
@Table(schema = "public", name = "ways")
class Way extends TWay {

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column
  var linestring: Geometry = _

  @Id
  @Column(name = "id")
  var id: Int = _

}
