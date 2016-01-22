package io.hgis.txt2shp

import com.vividsolutions.jts.geom.{LineString, Coordinate, GeometryFactory, Point}
import io.hgis.dump.{GeomType, ShapeWriter}

/**
  * Created by willtemperley@gmail.com on 22-Jan-16.
  */
object Car {

  val lines = scala.io.Source.fromFile("E:/tmp/unicode.txt")("UTF-16").getLines()
  val headerLine = lines.next()
  val headerNames = ("id", "chain_position", "type_of_material", "manufacturer_name", "coords", "country", "city", "transfer_type")
  val gf = new GeometryFactory()

  class TransitRoute {

    var id: Int = _

    var chainPosition: Int = _

    var startCoord: Coordinate = _

    var endCoord: Coordinate = _

    var name: String = _

    var materialType: String = _

  }

  def buildTransitRoute(line: String): TransitRoute = {

    val values = line.split("\t")

    val tr = new TransitRoute
    val coords = values(4).replaceAll("\"", "").split(",").map(_.toDouble)
    val coord = new Coordinate(coords(1), coords(0))

//    val pt = gf.createPoint(coord)
    tr.chainPosition = values(1).toInt
    tr.startCoord = coord
    tr.name = values(3)
//    tr.materialType =
    tr.id = values(0).toInt

    tr
  }


  def getRoutes: List[TransitRoute] = {

    //Avoid end lines and avoid Unknown cooords (id = 4)
    val routes = lines.filter(_.length > 10).filter(!_.contains("Unknown")).map(buildTransitRoute).toList


    def findEndCoord(transitRoute: TransitRoute): Coordinate = {

      //get the next in the chain
      val list = routes.filter(_.id == transitRoute.id).filter(_.chainPosition > transitRoute.chainPosition).sortBy(_.chainPosition)

//      if (list.nonEmpty) {
        list.head.startCoord
//      } else {
//        null
//      }

//      println(transitRoute.id)
//
//      null
    }
    routes.filter(_.chainPosition != 6).foreach(f => f.endCoord = findEndCoord(f))
    routes.filter(_.chainPosition != 6)
  }


  def main(args: Array[String]) {

//    writePoints("E:/tmp/test_points.shp")
    writeLines("E:/tmp/test_lines.shp")

  }
  def writeLines(fileName: String): Unit = {
    val sw = new ShapeWriter(geomType = GeomType.LineString, schemaDef = "name:String")

    val routes = getRoutes

    for (route <- routes) {
      val ls = gf.createLineString(Array(route.startCoord, route.endCoord))
      sw.addFeature(ls, Seq(route.name))
    }
    sw.write(fileName)
  }

  def writePoints(fileName: String): Unit = {

    val sw = new ShapeWriter(schemaDef = "name:String")

    for (line <- lines) {

      val values = line.split("\t")

      if (values.length > 4 && !values(4).equals("Unknown")) {

        val coords = values(4).replaceAll("\"", "").split(",").map(_.toDouble)

        val coord = new Coordinate(coords(1), coords(0))
        val pt = gf.createPoint(coord)
        sw.addFeature(pt, Seq(values(3)))
      }
    }
    sw.write(fileName)
  }
}
