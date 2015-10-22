package io.hgis.domain

import javax.persistence._

import com.esri.core.geometry.Geometry
import org.hibernate.annotations.Type

/**
 *
 * Created by tempehu on 01-Dec-14.
 */
@Entity
@Table(schema = "hgrid", name = "ecoregion_eez_protection_c")
class EcoregionEEZProtection extends EEPro {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
  @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.ecoregion_eez_protection_c_id_seq")
  var id: Long = _

}
