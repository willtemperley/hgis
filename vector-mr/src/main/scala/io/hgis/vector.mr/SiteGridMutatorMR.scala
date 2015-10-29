package io.hgis.vector.mr

//FIXME can we make this configurable to generically change a table??
/**
 *
 *
 * Changing structure of site-grid table slightly
 *
 *
 * Magic incantation:
 * HADOOP_CLASSPATH=$(hbase classpath) hadoop jar target/hbase-mr-0.1-SNAPSHOT-jar-with-dependencies.jar io.hgis.mr.GridLoaderMR <src> <dest>
 *
 * Created by willtemperley@gmail.com on 17-Nov-14.
 */

import com.esri.core.geometry.OperatorImportFromWkt
import io.hgis.ConfigurationFactory
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, Mapper}

object SiteGridMutatorMR {

  val catIdMap = Map("Ia" -> 1, "Ib" -> 2, "II" -> 3, "III" -> 4, "IV" -> 5, "V" -> 6, "VI" -> 7, "Not Assigned" -> 8, "Not Reported" -> 9)

  def main(args: Array[String]) {

    val conf= ConfigurationFactory.get

    val scan: Scan = new Scan
    scan.addFamily(SiteGridDAO.getCF)

    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    TableMapReduceUtil.initTableMapperJob("pa_grid", scan, classOf[SiteGridMutatorMR.PAMapper], classOf[ImmutableBytesWritable], classOf[Put], job)
    TableMapReduceUtil.addDependencyJars(job)

    /* No reducer required */
    TableMapReduceUtil.initTableReducerJob("pa_grid", null, job)
    job.waitForCompletion(true)

  }



  class PAMapper extends TableMapper[ImmutableBytesWritable, Put] {

    val wktImportOp = OperatorImportFromWkt.local()
    val outputKey = new ImmutableBytesWritable

    override def map(key: ImmutableBytesWritable, result: Result,
                     context: Mapper[ImmutableBytesWritable, Result, ImmutableBytesWritable, Put]#Context): Unit = {

      outputKey.set(key.get)
      val out = new Put(key.get)

//      val iucnCat = Bytes.toString(result.getValue("cfv".getBytes, SiteGridDAO.IUCN_CAT))

//      val catId = catIdMap(iucnCat)

//      out.add(SiteGridDAO.getCF, SiteGridDAO.CAT_ID, Bytes.toBytes(catId))

      context.write(outputKey, out)

    }

  }

}

