package io.hgis.domain
import javax.persistence._

import com.esri.core.geometry.Geometry
import io.hgis.vector.domain.GriddedEntity
import org.hibernate.annotations.Type

/**
 *
 * Created by willtemperley@gmail.com on 05-Feb-15.
 *
 */
trait EEPro extends GriddedEntity {

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

  @Transient
  var geom: Geometry = _

  @Column(name = "grid_id")
  var gridId: Int = _

  @Column(name = "ecoregion_eez_id")
  var analysisUnitId: Int = _

  @Column(name = "cat_id")
  var catId: Int = _

}
