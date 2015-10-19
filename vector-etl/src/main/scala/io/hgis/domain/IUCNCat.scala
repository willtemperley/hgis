package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import com.vividsolutions.jts.geom.Polygon
import io.hgis.vector.domain.TGridCell
import org.hibernate.annotations.Type

/**
 * Created by will on 24/10/2014.
 *
 *
 */
@Entity
@Table(schema = "protected_sites", name = "iucn_category")
class IUCNCat {

  @Id
  var id: String = _

  @Column
  var name: String = _

  @Column
  var description: String = _

  @Column(name = "ordinal")
  var catId: Int = _

}