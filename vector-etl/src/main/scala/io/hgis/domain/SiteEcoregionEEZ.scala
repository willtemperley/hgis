package io.hgis.domain

import javax.persistence.{Column, Entity, Id, Table, _}

import com.esri.core.geometry.Geometry
import io.hgis.vector.domain.TAnalysisUnitSite
import org.hibernate.annotations.Type

/**
 *
 * Created by will on 24/10/2014.
 */
@Entity
@Table(schema = "hgrid", name = "site_ecoregion_eez")
class SiteEcoregionEEZ extends TAnalysisUnitSite {

   @Id
   @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
   @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.site_ecoregion_eez_id_seq")
   var id: Integer = _

   @Column(name = "analysis_unit_id")
   override var entityId: Long = _

   @Column(name = "site_id")
   var siteId: Int = _

   @Type(`type` = "org.hibernate.spatial.GeometryType")
   @Column(name = "geom")
   var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

   @Transient
   override var geom: Geometry = _

}