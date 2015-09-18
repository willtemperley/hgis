package io.hgis.rasterize

import java.awt.Point
import java.awt.image.{BufferedImage, WritableRaster}
import java.io.IOException
import java.lang.Iterable
import javax.imageio.ImageIO

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.hgrid.GlobalGrid
import io.hgis.rasterize.Plotter
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.hbase.client.{Mutation, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableReducer, TableMapReduceUtil, TableMapper}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random

/*
*
*/
object RasterizeMR {

  val width: Int = 43200
  val height: Int = 21600
  val tileSize: Int = 1080

  val CFV: Array[Byte] = "cfv".getBytes
  val IMAGE_COL = "image".getBytes

  val highwayMap = Map(
    "motorway" -> 1,
    "trunk" -> 2,
    "primary" -> 3,
    "secondary" -> 4,
    "tertiary" -> 5,
    "motorway link" -> 6,
    "primary link" -> 7,
    "unclassified" -> 8,
    "road" -> 9,
    "residential" -> 10,
    "service" -> 11,
    "track" -> 12,
    "pedestrian" -> 13
  ).withDefaultValue(14)

  @throws(classOf[Exception])
  def main(args: Array[String]) {
    
    val conf = ConfigurationFactory.get

    val scan = new Scan
    scan.addFamily(CFV)
    scan.addFamily("cft".getBytes)

    val job = Job.getInstance(conf)
    job.setJarByClass(this.getClass)
    TableMapReduceUtil.addDependencyJars(job)

    TableMapReduceUtil.initTableMapperJob("ways", scan,
      classOf[WayRasterMapper], classOf[ImmutableBytesWritable], classOf[ImmutableBytesWritable], job)

//    job.setCombinerClass(classOf[MyTableCombiner])

    val clazz =
//    if (args.length > 1 && args(0).equals("precedence")) {
      classOf[PrecedenceReducer]
          println("Using Precedence")
//    } else {
//      println("Using Additive")
//      classOf[AdditiveReducer]
//    }

    //Reduces
    TableMapReduceUtil.initTableReducerJob("osm_tile", clazz, job)
    job.waitForCompletion(true)

  }

  /*
   *  A rasterizer with additive behaviour
   */
  class AdditiveReducer extends ImageTileReducer {
    override def getPlotter(ras: WritableRaster): Plotter = {
      new AdditivePlotter(ras)
    }
  }

  /*
   *  A rasterizer which takes pixel with the highest precedence
   */
  class PrecedenceReducer extends ImageTileReducer {
    override def getPlotter(ras: WritableRaster): Plotter = {
      new PrecedencePlotter(ras)
    }
  }



  /**
   * Maps each pixel in a rasterized line to it's corresponding tile.
   *
   * The tile id is a concatenation of the tile's origin, in pixel coordinates.
   *
   */
  class WayRasterMapper extends TableMapper[ImmutableBytesWritable, ImmutableBytesWritable] {

    val wkbReader = new WKBReader()
    
    val gridKey = new ImmutableBytesWritable()

    val pixel = new ImmutableBytesWritable()

    val wkt = new WKTReader

    val grid = new GlobalGrid(width, height, tileSize)

    val highwayColumn = AccessUtil.stringColumn("cft", "highway") _

    // Keeping these constant prevents continunal re-evaluation of getBytes
    val GEOM = "geom".getBytes
    val CFV = "cfv".getBytes
    val CFT = "cft".getBytes

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable]#Context): Unit = {

      val wkb = result.getValue(CFV, GEOM)
      val hwy = highwayColumn(result)

      //Send the pixel info to a grid id
      val plotter = new  Plotter {

        val pix = new Array[Int](1)

        override def plot(x: Int, y: Int): Unit = {

          gridKey.set(grid.gridId(x, y))
          val bytes = grid.toBytes(x, y)

          pixel.set(bytes ++ Bytes.toBytes(highwayMap(hwy)))
          context.write(gridKey, pixel)

        }

        override def setValue(v: Int): Unit = {
          pix(0) = v
        }
      }

      if (wkb != null){

        val geom = wkbReader.read(wkb)

        val coords = geom.getCoordinates

        //Sliding iterates the gaps in the fence :)
        val slide: Iterator[Array[Coordinate]] = coords.sliding(2)
        for (pair <- slide) {

          val a = pair(0)
          val b = pair(1)

          val a1: (Int, Int) = grid.snap(a.getOrdinate(0), a.getOrdinate(1))
          val b1: (Int, Int) = grid.snap(b.getOrdinate(0), b.getOrdinate(1))

          Rasterizer.rasterize(a1._1, a1._2, b1._1, b1._2, plotter)
        }

      }
    }
  }



  /**
   * Sets or increments a pixel in a raster
   *
   * @param raster a JAI raster
   */
  class AdditivePlotter(raster: WritableRaster) extends Plotter {

    val pix = new Array[Int](1)

    def plot(x: Int, y: Int) {

      raster.getPixel(x, y, pix)
      pix(0) = pix(0) + 1
      raster.setPixel(x, y, pix)

    }

    override def setValue(v: Int): Unit = pix(0) = v
  }

  /**
   * If a pixel has a higher precedence it's set, otherwise ignored
   *
   * @param raster a JAI raster
   */
  class PrecedencePlotter(raster: WritableRaster) extends Plotter {

    val p1 = new Array[Int](1)
    val p2 = new Array[Int](1)

    def plot(x: Int, y: Int) {

      raster.getPixel(x, y, p1)
      if (p2(0) > p1(0)) {
        raster.setPixel(x, y, p2)
      }

    }

    override def setValue(v: Int): Unit = p2(0) = v
  }



  /**
   * Receives a tile ID and the correct pixels for the tile, which are plotted and output to an HBase row as a PNG image
   *
   */
  abstract class ImageTileReducer extends TableReducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable] {

    val grid = new GlobalGrid(width, height, tileSize)

    override def reduce(key: ImmutableBytesWritable,
                        values: Iterable[ImmutableBytesWritable],
                        context: Reducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation]#Context): Unit = {

      val put = new Put(key.get())

      val bos = new ByteArrayOutputStream()

      val image: BufferedImage = new BufferedImage(tileSize, tileSize, BufferedImage.TYPE_BYTE_GRAY)

      val ras: WritableRaster = image.getRaster

      val plotter = getPlotter(ras)

      val orig = grid.gridIdToOrigin(key.get())

      for (value <- values) {
        val bytes = value.get()
        val pix = grid.keyToPixel(bytes)


        val x = pix._1 - orig._1
        val y = pix._2 - orig._2
        if (x >= 0 && x < tileSize && y >= 0 && y < tileSize) {

          //Input can be 8 or 12 bytes
          if (bytes.length == 12){
            plotter.setValue(Bytes.toInt(bytes.slice(8,12)))
          }

          plotter.plot(x, y)
        } else {
          //Just to debug
          val k = new Array[Byte](4)
          Random.nextBytes(k)
          val put = new Put(k)
          val message = "Pixel=" + pix._1 + ":" + pix._2  + "; " + "x=" + x + " y=" + y
          put.add(CFV, "error".getBytes, message.getBytes)
          context.write(null, put)
        }

      }

      ImageIO.write(image, "png", bos)
  
      val bytes = bos.toByteArray

      put.add(CFV, IMAGE_COL, bytes)

      context.write(null, put)
    }

    def getPlotter(ras: WritableRaster): Plotter

  }

  //  class MyTableCombiner extends Reducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable] {
  //
  //    val pixel = new ImmutableBytesWritable()
  //
  ////    private final IntWritable iw = new IntWritable();
  //    override def reduce(key: ImmutableBytesWritable,
  //                    values: Iterable[ImmutableBytesWritable],
  //                    context: Reducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable]#Context): Unit = {
  //
  //        val x = values.map(_.get()).toList.distinct
  //        for (y <- x) {
  //          pixel.set(y)
  //          context.write(key, pixel)
  //        }
  ////      values.toList.distinct.foreach(v => context.write(key, v))
  //
  //    }
  //  }
}

