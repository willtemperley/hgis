package io.hgis.vector.domain

import com.vividsolutions.jts.geom.Geometry
import io.hgis.hdomain.HasRowKey
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TGridNode extends TGridCell with HasRowKey {

  var parentId: Int

  var isLeaf: Boolean

}
