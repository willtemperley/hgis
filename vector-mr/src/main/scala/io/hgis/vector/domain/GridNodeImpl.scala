package io.hgis.vector.domain

import com.esri.core.geometry.Geometry

/**
  * Created by willtemperley@gmail.com on 17-Dec-15.
  */
@SerialVersionUID(-1235401983450148l)
class GridNodeImpl extends TGridNode with Serializable {

  override var parentId: Int = _

  override var isLeaf: Boolean = _

  override var geom: Geometry = _

  override var gridId: Int = _

}
