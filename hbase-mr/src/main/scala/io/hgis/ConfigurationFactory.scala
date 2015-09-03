package io.hgis

import java.util.Properties

import org.apache.hadoop.conf.Configuration

import collection.JavaConversions._

/**
 * Just a place to keep the config in one place
 *
 * Created by tempehu on 22-Dec-14.
 */
object ConfigurationFactory {



  def get: Configuration = {


    val props = new Properties()

    val loader = Thread.currentThread().getContextClassLoader

    val resourceStream = loader.getResourceAsStream("hbase-config.properties")
    props.load(resourceStream)

    //    conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-02,hadoop-m1")
    //    conf.set("hbase.zookeeper.clientPort", "2181")
    //    conf.set("hbase.master", "hadoop-m1")

    val conf = new Configuration
    props.keys().foreach(f => conf.set(f.toString, props.get(f.toString).toString))

    conf
  }

}
