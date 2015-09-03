package io.hgis.scanutil

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64

/**
 * Multi-scans have to be serialized to strings and no utility seems to exist to do this.
 *
 * Created by tempehu on 11-Dec-14.
 */
object MultiScan {

  def serializeScans(scans: Array[Scan]): String = {
    scans.map(encodeScan).mkString(",")
  }

  def encodeScan(scan: Scan): String = {
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
  }

}
