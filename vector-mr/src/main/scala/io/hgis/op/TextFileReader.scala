package io.hgis.op

import java.io.File

import org.apache.hadoop.fs.Path

import scala.io.Source

/**
  * Created by willtemperley@gmail.com on 30-Nov-15.
  */
class TextFileReader(file: File) {

  val example = "src/main/resources/OSM_speed_table_by_country_v1.csv"

  val pt = new Path("hdfs:/user/tempehu/gridcells.dat")

  val lines = Source.fromFile(file).getLines()
  val head = lines.next().split(',')
  val tab = lines.map((f: String) => f.split(',')).map(head.zip(_).toMap).toList

}
