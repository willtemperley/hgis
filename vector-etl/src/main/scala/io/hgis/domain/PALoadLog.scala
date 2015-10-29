package io.hgis.domain

import javax.persistence.{Column, _}

/**
 *
 * Created by will on 24/10/2014.
 */
@Entity
@Table(schema = "hgrid", name = "pa_load_log")
class PALoadLog {

  @Id
  @Column(name = "site_id")
  var siteId: Long = _

  @Column(name = "is_loaded")
  var isLoaded: Boolean = _

}
