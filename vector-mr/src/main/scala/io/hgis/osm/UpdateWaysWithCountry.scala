package io.hgis.osm

/**
  */

import java.io.ObjectInputStream

import com.esri.core.geometry
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.hdomain.{ConvertsGeometry, AnalysisUnitDAO, AnalysisUnit}
import io.hgis.op.{IntersectUtil, HTree}
import io.hgis.scanutil.TableMapReduceUtilFix
import io.hgis.vector.domain.TGridNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper}
import org.apache.hadoop.mapreduce.{Job, Mapper}

/*
http://hbase.apache.org/book/mapreduce.example.html
 */
object UpdateWaysWithCountry {

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val conf: Configuration = ConfigurationFactory.get

    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
    scan.addFamily("cft".getBytes)

    val filter = new SingleColumnValueFilter("cft".getBytes, "maxspeed".getBytes, CompareOp.NOT_EQUAL, new BinaryComparator(null))
    filter.setFilterIfMissing(true)
    scan.setFilter(filter)

    val job: Job = Job.getInstance(conf)

    TableMapReduceUtil.addDependencyJars(job)
    job.setJarByClass(this.getClass)

    val scans = List(
      getScanForTable("transport"),
      getScanForTable("country") //fixme doesn't yet exist
    )


    //Maps the site grid
    TableMapReduceUtilFix
      .initTableMapperJob(scans,
        classOf[WayCountryIXMapper],
        classOf[ImmutableBytesWritable],
        classOf[Result],
        job, addDependencyJarsB = true, initCredentials = false)
    TableMapReduceUtil.addDependencyJars(job)

//    TableMapReduceUtil.initTableMapperJob("transport", scan,
//      classOf[UpdateWaysWithCountry.WayExtractMapper], classOf[ImmutableBytesWritable], classOf[Put], job)

    TableMapReduceUtil.initTableReducerJob("FIXME", null, job)
    job.waitForCompletion(true)

  }

  def getScanForTable(tableName: String): Scan = {
    val scan: Scan = new Scan
    scan.addFamily("cfv".getBytes)
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getBytes)
    scan
  }

  /**
   *
   *
   */
  class WayCountryIXMapper extends  TableMapper[ImmutableBytesWritable, Put] {

    val emittedKey = new ImmutableBytesWritable()

//    val getGeom = AccessUtil.geomColumn(new WKBReader(), "cfv", "geom") = _
    val geomColumn  = AccessUtil.geomColumn(new WKBReader(), "cfv", "geom") _

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

//      emittedKey.set(key.get())
//      val put = new Put(emittedKey.get())

      val geom = geomColumn(result)

      val node = null //fixme hTree.findSmallestContainingNode(geom.getEnvelope)
      val leaves = hTree.findLeaves(node)

      val au = AnalysisUnitDAO.fromResult(result)

      //These are the gridded ways to map
      val grids = IntersectUtil.executeIntersect(au,
        leaves.map(_.geom).toArray,
        leaves.map(_.gridId).toArray,
        3)




//      result.rawCells().foreach(put.add)
//      context.write(emittedKey, put)

    }


    var hTree: HTree = _

    override def setup(context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      val pt = new Path("hdfs:/user/tempehu/gridcells.dat")
      val fs = FileSystem.get(new Configuration())

      val is = new ObjectInputStream(fs.open(pt))
      val gridCells  = is.readObject().asInstanceOf[Array[TGridNode]]
      hTree = new HTree(gridCells, gridCells(0))

      is.close()
    }
  }


}

