package io.hgis.vector.domain.gen

import com.esri.core.geometry.Geometry

/**
 * Created by willtemperley@gmail.com on 15-Oct-15.
 */
trait GriddedEntity {

  var gridId: Int
  var geom: Geometry
  var jtsGeom: com.vividsolutions.jts.geom.Geometry
}
