package io.hgis.load

import java.nio.ByteBuffer

import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorImportFromWkb, OperatorImportFromWkt}
import com.vividsolutions.jts.geom
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, WKTWriter}

/**
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
trait ConvertsGeometry {

  val jtsWktWriter = new WKTWriter

  val jtsWkbWriter = new WKBWriter
  val jtsWkbReader = new WKBReader

  val esriWkbReader = OperatorImportFromWkb.local()
  val esriWkbWriter = OperatorExportToWkb.local()

  val wktImportOp = OperatorImportFromWkt.local()

  def jtsToEsri(jtsGeom: geom.Geometry, esriGeomType: Geometry.Type = Geometry.Type.Polygon): Geometry = {
    esriWkbReader.execute(0, esriGeomType, ByteBuffer.wrap(jtsWkbWriter.write(jtsGeom)), null)
  }
  def esriToJTS(esriGeom: Geometry): geom.Geometry = {
    jtsWkbReader.read(esriWkbWriter.execute(0, esriGeom, null).array())
  }
}
