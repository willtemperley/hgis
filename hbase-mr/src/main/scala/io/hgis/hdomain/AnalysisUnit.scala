package io.hgis.hdomain

import com.esri.core.geometry.Geometry

/**
 *
 *
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait AnalysisUnit  {

  var entityId: Long

  var geom: Geometry

  var jtsGeom: com.vividsolutions.jts.geom.Geometry

}
