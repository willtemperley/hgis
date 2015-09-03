package io.hgis.rasr

import java.awt.image.{BufferedImage, WritableRaster}
import java.lang.Iterable
import javax.imageio.ImageIO

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import io.hgis.ConfigurationFactory
import io.hgis.hgrid.GlobalGrid
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.hbase.client.{Mutation, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper, TableReducer}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._
import scala.util.Random

/*
*
*/
object RasterizeMR {


  val width: Int = 51200
  val height: Int = 25600
  val tileSize: Int = 1024

  val CFV: Array[Byte] = "cfv".getBytes
  val IMAGE_COL = "image".getBytes

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

    //Reduces
    TableMapReduceUtil.initTableReducerJob("osm_tile", classOf[ImageTileReducer], job)
    job.waitForCompletion(true)

    job.waitForCompletion(true)

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

    // Keeping these constant prevents continunal re-evaluation of getBytes
    val GEOM = "geom".getBytes
    val CFV = "cfv".getBytes

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable]#Context): Unit = {

      val wkb = result.getValue(CFV, GEOM)

      //Send the pixel info to a grid id
      val plotter = new  Plotter {
        override def plot(x: Int, y: Int): Unit = {
          gridKey.set(grid.gridId(x, y))
          pixel.set(grid.toBytes(x,y))
          context.write(gridKey, pixel)
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
   * Plots a binary image
   *
   * @param raster a JAI raster
   */
  class WritableRasterPlotter(raster: WritableRaster) extends Plotter {

    def plot(x: Int, y: Int) {

        raster.setPixel(x, y, Array[Int](1))

    }
  }


  /**
   * Receives a tile ID and the correct pixels for the tile, which are plotted and output to an HBase row as a PNG image
   *
   */
  class ImageTileReducer extends TableReducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable] {

    val grid = new GlobalGrid(width, height, tileSize)

    override def reduce(key: ImmutableBytesWritable,
                        values: Iterable[ImmutableBytesWritable],
                        context: Reducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation]#Context): Unit = {

      val put = new Put(key.get())

      val bos = new ByteArrayOutputStream()

      val image: BufferedImage = new BufferedImage(tileSize, tileSize, BufferedImage.TYPE_BYTE_GRAY)

      val ras: WritableRaster = image.getRaster
      val plotter = new WritableRasterPlotter(ras)

      for (value <- values) {
        val pix = grid.keyToPixel(value.get())
        val orig = grid.gridIdToOrigin(key.get())
        val x = pix._1 - orig._1
        val y = pix._2 - orig._2
        if (x >=0 && x < tileSize && y >= 0 && y < tileSize) {
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
  }

}

