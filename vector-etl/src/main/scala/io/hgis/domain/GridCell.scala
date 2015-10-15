package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.geom.Polygon
import io.hgis.vector.domain.TGridCell
import org.hibernate.annotations.Type

/**
 * Created by will on 24/10/2014.
 *
 * An instance of a Hierarchical grid cell
 *
 */
@Entity
@Table(schema = "hgrid", name = "h_grid")
class GridCell extends TGridCell {

   @Id
   @Column(name = "id")
   @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
   @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.h_grid_id_seq")
   override var gridId: Int = _

   @Type(`type` = "org.hibernate.spatial.GeometryType")
   @Column(name = "geom")
   var jtsGeom: Polygon = _

   @Transient
   override var geom: Geometry = _

}