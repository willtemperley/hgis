package io.hgis.domain

import javax.persistence.{Column, _}

/**
  *
  * Created by will on 24/10/2014.
  */
@Entity
@Table(schema = "hgrid", name = "load_queue")
class LoadQueue {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
  @SequenceGenerator(allocationSize = 1, name = "seq", sequenceName = "hgrid.load_queue_id_seq")
  var id: Long = _

  @Column(name = "entity_id")
  var entityId: Long = _

  @Column(name = "entity_type")
  var entityType: String = _

  @Column(name = "is_loaded")
  var isLoaded: Boolean = _

}
