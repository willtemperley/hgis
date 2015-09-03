package io.hgis.scanutil

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{MultiTableInputFormat, TableMapReduceUtil, TableMapper}
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.mapreduce.Job

/**
 * For some bizarre reason [[TableMapReduceUtil]] limits the outputKeyClass and outputValueClass
 * This causes issues hence this class.
 *
 * Fixed by: https://issues.apache.org/jira/browse/HBASE-12929 but won't arrive until HBase 1.0
 */
object TableMapReduceUtilFix {

  def initTableMapperJob(scans: List[Scan],
                         mapper: Class[_ <: TableMapper[_, _]],
                         outputKeyClass: Class[_ <: WritableComparable[_]],
                         outputValueClass: Class[_],
                         job: Job,
                         addDependencyJarsB: Boolean,
                         initCredentials: Boolean) = {
    job.setInputFormatClass(classOf[MultiTableInputFormat])
    if (outputValueClass != null) {
      job.setMapOutputValueClass(outputValueClass)
    }
    if (outputKeyClass != null) {
      job.setMapOutputKeyClass(outputKeyClass)
    }
    job.setMapperClass(mapper)
    val conf = job.getConfiguration
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))

    val scanList: List[String] = scans.map(MultiScan.encodeScan)

    job.getConfiguration.setStrings(MultiTableInputFormat.SCANS, scanList.toArray.mkString(","))

    if (addDependencyJarsB) TableMapReduceUtil.addDependencyJars(job)
    if (initCredentials) TableMapReduceUtil.initCredentials(job)

  }
}
