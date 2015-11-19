package io.hgis.domain.rl

import javax.persistence._

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.{GriddedEntity, AnalysisUnit}
import org.hibernate.annotations.Type

/**
 * Maps the country table
 *
 * Created by tempehu on 01-Dec-14.
 */

//CREATE TABLE ehabitat.temp_dissolved_common_redpoll
//(
//id_no integer,
//binomial text,
//rl_update double precision,
//scientific_name text,
//comm_name text,
//kingdom text,
//phylum text,
//class text,
//order_ text,
//family text,
//genus_name text,
//species_name text,
//category text,
//biome_mar text,
//biome_fw text,
//biome_terr text,
//geom geometry
//)

@Entity
@Table(schema = "hgrid", name = "species_grid")
class SpeciesGrid extends GriddedEntity {

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

  @Transient
  override var geom: Geometry = _

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
  @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.spp_grid_id_seq")
  var id: Int = _

  @Column(name = "species_id")
  override var entityId: Long = _

  @Column(name = "grid_id")
  override var gridId: Int = _

}
