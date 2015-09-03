package io.hgis.dump

import org.apache.hadoop.hbase.client.{Result, ResultScanner}

/**
 * Created by willtemperley@gmail.com on 05-Jun-15.
 */
trait GeometryScanner {

  def getIterator(scanner: ResultScanner): Iterator[Result] = {
    Iterator.continually(scanner.next).takeWhile(_ != null)
  }

}
