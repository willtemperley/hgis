package io.hgis.domain.rl

import javax.persistence._

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.AnalysisUnit
import org.hibernate.annotations.{Immutable, Type}

/*

 */
@Entity
@Immutable
@Table(schema = "hgrid", name = "test_species")
@SerialVersionUID(-132459812709L)
class TestSpecies extends AnalysisUnit with Serializable {

  @Type(`type` = "org.hibernate.spatial.GeometryType")
  @Column(name = "geom")
  var jtsGeom: com.vividsolutions.jts.geom.Geometry = _

  @Transient
  override var geom: Geometry = _

  @Column(name = "binomial")
  var binomial: String = _


  @Id
  @Column(name = "ogc_fid")
  var ogcFid: Long = _

  @Column(name = "id_no")
  override var entityId: Long = _

  @Column
  var kingdom: String = _

  @Column
  var phylum: String = _

  @Column(name = "class")
  var clazz: String = _

  @Column(name = "order_")
  var order: String = _

  @Column
  var family: String = _

  @Column(name = "genus_name")
  var genus: String = _

  @Column(name = "species_name")
  var speciesName: String = _

  @Column
  var category: String = _

  @Column(name = "area_km2_g")
  var areaKm2g: Double = _

  @Column(name = "area_km2_m")
  var areaKm2m: Double = _

}
