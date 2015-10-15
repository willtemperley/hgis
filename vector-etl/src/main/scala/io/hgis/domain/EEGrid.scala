package io.hgis.domain

import javax.persistence.{Column, Entity, Id, Table, _}
import javax.validation.constraints.NotNull

import org.hibernate.annotations.Type

/**
 *
 * Created by will on 24/10/2014.
 */
@Entity
@Table(schema = "hgrid", name = "ee_grid")
class EEGrid  {

   @Id
   @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
   @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.ee_grid_id_seq")
   var id: Integer = _

   @NotNull
   @Column(name = "grid_id")
   var gridId: Int = _

   @Type(`type` = "org.hibernate.spatial.GeometryType")
   @NotNull
   @Column(name = "geom")
   var geom: com.vividsolutions.jts.geom.Geometry = _

   @NotNull
   @Column(name = "ee_id")
   var eeId: Int = _

}