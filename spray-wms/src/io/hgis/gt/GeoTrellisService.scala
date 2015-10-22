package io.hgis.gt

import java.awt.image.BufferedImage
import java.awt.{Graphics2D, Rectangle}
import java.net.URL
import javax.imageio.ImageIO
import javax.persistence.EntityManager

import com.vividsolutions.jts.geom.{GeometryFactory, LineString}
import io.hgis.domain.osm.WayWMS
import org.apache.commons.io.output.ByteArrayOutputStream
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.map._
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.renderer.lite.StreamingRenderer
import org.geotools.styling.{SLDParser, Style, StyleFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import spray.http.{HttpData, HttpEntity, MediaTypes}
import spray.routing.HttpService

import scala.collection.JavaConversions._
/**
 * Created by tempehu on 27-Apr-15.
 */


trait GeoTrellisService extends HttpService {

  def pingRoute = path("ping") {
    get { complete("pong!") }
  }

  def pongRoute = path("pong") {
    get { complete("pong!?") }
  }

  val em: EntityManager

  def rasterRoute = path("wms") {
    parameters(
      'version ? 1.1,
      'styles ? "",
      'width ? 256,
      'height ? 256,
      'bbox
    ) { (version, styles, width, height, bbox) =>
      get {
        val coords = bbox.split(",").map(_.toDouble)
        val crs = DefaultGeographicCRS.WGS84 // set crs first
        //-180 -90 180 90 v 1.1
        val env = new ReferencedEnvelope(coords(0), coords(2), coords(1), coords(3), crs)
        complete(HttpEntity(MediaTypes.`image/png`, HttpData(getPic(width, height, env))))
      }
    }
  }

  def getQ(referencedEnvelope: ReferencedEnvelope): List[WayWMS] = {
    val gf = new GeometryFactory
    val g = gf.toGeometry(referencedEnvelope)
    g.setSRID(4326)
    val q =em.createQuery("from WayWMS w where intersects(w.geom, :filter) = true", classOf[WayWMS]).setParameter("filter", g)
    q.getResultList.toList
  }

  def wayToFeature(way: WayWMS): SimpleFeature = {
    val WAY: SimpleFeatureType = getSimpleFeatureType
    //create the builder
    val builder = new SimpleFeatureBuilder(WAY)

    //add the values
    builder.add(way.ref)
    builder.add(way.highway)
    builder.add(way.name)
    builder.add(way.amenity)
    builder.add(way.tunnel)
    builder.add(way.railway)
    builder.add(way.geom)

    //build the feature with provided ID
    val x = builder.buildFeature("fid." + way.entityId)


    x

  }

  //FIXME cache
  def getSimpleFeatureType: SimpleFeatureType = {
    val b = new SimpleFeatureTypeBuilder()
    //set the name
    b.setName("Way")
    b.add("ref", classOf[String])
    b.add("highway", classOf[String])
    b.add("name", classOf[String])
    b.add("amenity", classOf[String])
    b.add("tunnel", classOf[String])
    b.add("railway", classOf[String])

    //add a geometry property
    b.setCRS(DefaultGeographicCRS.WGS84) // set crs first
    b.add("geometry", classOf[LineString]) // then add geometry
    b.setDefaultGeometry("geometry")
    //build the type
    val WAY = b.buildFeatureType()
    WAY
  }

  def getPic(width: Int, height: Int, envelope: ReferencedEnvelope): Array[Byte] = {

    val styleFactory: StyleFactory = CommonFactoryFinder.getStyleFactory(null)

    val spec: String = "file:///E:/IdeaProjects/hgis/spray-wms/data/osm.sld"
//    val spec = "file:///E:/tmp/geotools-test/simpleline.sld"

    val parser: SLDParser = new SLDParser(styleFactory, new URL(spec))
    val style: Style = parser.readXML.toList.head
//    val shapefile: ShapefileDataStore = new ShapefileDataStore(new URL("file:///E:/tmp/geotools-test/Export_Output.shp"))

//    val featureCollection  = shapefile.getFeatureSource(shapefile.getTypeNames.toList.head)
    val featureCollection: ListFeatureCollection = getFeatureSource(envelope)

    //    val featureSource2: HFeatureStore = getFeatureSource(envelope)
    val fl = new FeatureLayer(featureCollection, style)
    val content: MapContent = new MapContent();//(Array[MapLayer](new DefaultMapLayer(featureSource, style)))

    content.addLayer(fl)

    val image: BufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    val graphics: Graphics2D = image.createGraphics
    val screenArea: Rectangle = new Rectangle(0, 0, width, height)

    val renderer: StreamingRenderer = new StreamingRenderer
    renderer.setMapContent(content)
//    renderer.setFeatures(featureCollection)
    renderer.paint(graphics, screenArea, envelope)
//        renderer.paint(graphics, screenArea, mapArea)

    val baos = new ByteArrayOutputStream()
    ImageIO.write(image, "png", baos)
    content.dispose()
    baos.toByteArray
  }

  def getFeatureSource(envelope: ReferencedEnvelope): ListFeatureCollection = {
    val features = getQ(envelope).map(f => wayToFeature(f))

    val featureColl = new ListFeatureCollection(getSimpleFeatureType)
    for (f <- features) {
      featureColl.add(f)
    }
//    val featureSource = DataUtilities.source(featureColl)
//    val y = new ContentEntry();
//    val hfs = new HFeatureStore()
    featureColl
  }

  def rootRoute = pingRoute ~ pongRoute ~ rasterRoute
}