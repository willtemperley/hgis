package io.hgis.hdomain

import org.apache.hadoop.hbase.client.{Put, Result}

/**
 * Methods to create a Put and decode a Result, to and from domain objects
 *
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait HSerializable[T] {

  /**
   * Generates a Put from a domain object
   *
   * @param obj the domain object
   * @param rowKey the row key for the domain object
   * @return
   */
  def toPut(obj: T, rowKey: Array[Byte]): Put


  /**
   * Decodes a result into a domain object
   *
   * @param result An HBase row result
   * @return A domain object of type T
   */
  def fromResult(result: Result, domainObj: T): T

  /**
   * The column family this domain object uses
   *
   * @return
   */
  def getCF: Array[Byte]
}
