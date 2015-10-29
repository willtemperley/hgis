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

    val conf = new Configuration
    props.toMap.foreach(f => conf.set(f._1, f._2))

    conf
  }

}
