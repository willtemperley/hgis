package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.GriddedEntity
import org.hibernate.annotations.Type

/**
 * Created by willtemperley@gmail.com on 22-Oct-15.
 */
@Entity
@Table(schema = "hgrid", name = "way_grid")
class WayGrid extends GriddedEntity{

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
  @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.way_grid_id_seq")
  var id: Integer = _

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

  @Transient
  var geom: Geometry = _

  @Column(name = "grid_id")
  var gridId: Int = _

  @Column(name = "entity_id")
  var entityId: Long = _

}
