package io.hgis.domain

import javax.persistence.{Column, Entity, Id, Table, _}
import javax.validation.constraints.NotNull

import com.esri.core.geometry.Geometry
import io.hgis.vector.domain.TSiteGrid
import org.hibernate.annotations.Type

/**
 *
 * The mapping between sites and PAs
 *
 * Created by will on 24/10/2014.
 */
@Entity
@Table(schema = "hgrid", name = "site_grid")
class SiteGrid extends TSiteGrid {

   @Id
   @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
   @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.site_grid_geom_id_seq")
   var id: Integer = _

   @NotNull
   @Column(name = "site_id")
   override var entityId: Int = _

   @NotNull
   @Column(name = "grid_id")
   override var gridId: Int = _

   @Transient
   override var geom: Geometry = _

   @Type(`type` = "org.hibernate.spatial.GeometryType")
   @NotNull
   @Column(name = "site_geom")
   var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

   @NotNull
   @Column(name = "iucn_cat")
   override var iucnCat: String = _

   @NotNull
   @Column(name = "is_designated")
   override var isDesignated: Boolean = _

   @NotNull
   @Column(name = "cat_id")
   override var catId: Int = _
}