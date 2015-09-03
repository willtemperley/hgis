package io.hgis.domain

import javax.persistence.{Column, Entity, Id, Table, _}

import com.vividsolutions.jts.geom.Point
import org.hibernate.annotations.Type

/**
 *
 * The mapping between sites and PAs
 *
 * Created by will on 24/10/2014.
 */
@Entity
@Table(schema = "hgrid", name = "site_hash")
class SiteHash {

   @Id
   @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
   @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.site_hash_id_seq")
   var id: Integer = _

   @Column(name = "site_id")
   var siteId: Int = _

   @Column
   var geohash: String = _

   @Type(`type` = "org.hibernate.spatial.GeometryType")
   @Column(name = "centroid")
   var geom: Point = _

}