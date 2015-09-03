package io.hgis;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.map.*;
import org.opengis.feature.simple.SimpleFeatureType;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.net.URL;
import javax.imageio.ImageIO;
import org.geotools.data.FeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.renderer.lite.StreamingRenderer2;
import org.geotools.styling.SLDParser;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Created by tempehu on 17-Apr-15.
 */


public class App {


//    public static void main(String[] args) throws Exception {
   public static void x() throws Exception {

        StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory(null);

//        String spec = "file:///E:/tmp/geotools-test/popshade.sld";
        String spec = "file:///E:/IdeaProjects/hgis/spray-wms/data/osm.sld";

        SLDParser parser = new SLDParser(styleFactory, new URL(spec));

        Style style = parser.readXML()[0];

        ShapefileDataStore shapefile =
                new ShapefileDataStore(new URL("file:///E:/tmp/geotools-test/Export_Output.shp"));
        FeatureSource<SimpleFeatureType, SimpleFeature> features =
                shapefile.getFeatureSource(shapefile.getTypeNames()[0]);

        MapContext context = new DefaultMapContext(new MapLayer[]{
                new DefaultMapLayer(features, style)
        });

        BufferedImage image = new BufferedImage(600, 600, BufferedImage.TYPE_INT_ARGB);
        Graphics2D graphics = image.createGraphics();
        Rectangle screenArea = new Rectangle(0, 0, 600, 600);
        ReferencedEnvelope mapArea = features.getBounds();

        StreamingRenderer2 renderer = new StreamingRenderer2();
        renderer.setContext(context);
        renderer.paint(graphics, screenArea, mapArea);

        ImageIO.write(image, "png", new File("E:/tmp/states.png"));

    }
}