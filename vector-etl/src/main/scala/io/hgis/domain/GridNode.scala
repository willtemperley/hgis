package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.geom.Polygon
import io.hgis.vector.domain.{TGridNode, TGridCell}
import org.hibernate.annotations.Type

/**
 * Created by will on 24/10/2014.
 *
 * An instance of a Hierarchical grid cell
 *
 */
@Entity
@Table(schema = "hgrid", name = "h_grid2")
@SerialVersionUID(-298134123498915l)
class GridNode extends TGridNode with Serializable {

   @Id
   @Column(name = "id")
   override var gridId: Int = _

   @Type(`type` = "org.hibernate.spatial.GeometryType")
   @Column(name = "geom")
   var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

   @Transient
   override var geom: Geometry = _

   @Column(name = "parent_id")
   override var parentId: Int = _

   @Column(name = "is_leaf")
   override var isLeaf: Boolean = false

}
