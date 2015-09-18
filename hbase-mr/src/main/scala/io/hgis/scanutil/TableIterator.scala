package io.hgis.scanutil

import org.apache.hadoop.hbase.client.{Result, ResultScanner}

/**
 * Utility function for obtaining an iterator for a table
 *
 * Created by willtemperley@gmail.com on 05-Jun-15.
 */
trait TableIterator {

  def getIterator(scanner: ResultScanner): Iterator[Result] = {
    Iterator.continually(scanner.next).takeWhile(_ != null)
  }

}
