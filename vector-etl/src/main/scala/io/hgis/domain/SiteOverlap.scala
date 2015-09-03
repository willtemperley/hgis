package io.hgis.domain

import javax.persistence.{Column, Entity, Id, Table, _}

import io.hgis.vector.domain.TSiteOverlap

/**
 *
 * The mapping between sites and PAs
 *
 * Created by will on 24/10/2014.
 */
@Entity
@Table(schema = "hgrid", name = "site_overlap")
class SiteOverlap extends TSiteOverlap {

   @Id
   @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
   @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.site_grid_geom_id_seq")
   var id: Integer = _

   @Column(name = "site_id1")
   override var siteId1: Int = _

   @Column(name = "site_id2")
   override var siteId2: Int = _

   @Column(name = "area")
   override var area: Double = _

}