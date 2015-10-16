package io.hgis.vector.domain

import com.esri.core.geometry.Geometry

/**
 *
 *
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait AnalysisUnit  {

  var entityId: Int

  var geom: Geometry

  var jtsGeom: com.vividsolutions.jts.geom.Geometry

}
