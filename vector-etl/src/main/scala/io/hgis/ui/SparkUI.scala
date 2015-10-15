import org.apache.hadoop.hbase.filter.SingleColumnValueFilter

//package io.hgis.ui

//import com.google.inject.Guice
//import com.vaadin.server.VaadinRequest
//import com.vaadin.ui.Button.{ClickEvent, ClickListener}
//import com.vaadin.ui.{Button, Label, UI, VerticalLayout}
//import com.vividsolutions.jts.geom.Geometry
//import com.vividsolutions.jts.io.WKBReader
//import io.hgis.inject.ConfigModule
////import org.apache.hadoop.conf.Configuration
////import org.apache.hadoop.hbase.client.{HTable, Result}
////import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}


//class SparkUI extends UI{
//
////  private val rdd = getSC
//
//  override def init(vaadinRequest: VaadinRequest): Unit = {
//
//    val l = new Label("Test")
//    val vl = new VerticalLayout()
//    setContent(vl)
//    vl.addComponent(l)
//    val button = new Button()
//    button.addClickListener(new ClickListener {
//      override def buttonClick(clickEvent: ClickEvent): Unit = {
////        l.setValue("RDD count: " + doCount().toString)
//      }
//    })
//    vl.addComponent(button )
//
//  }
//
//  val injector = Guice.createInjector(new ConfigModule(prod = true))
//

//  def getSC: RDD[(ImmutableBytesWritable, Result)] = {
//
//    val configuration = injector.getInstance(classOf[Configuration])
//
//    configuration.addResource("hbase-site-local.xml")
//    configuration.reloadConfiguration()
//
//    val tableName = "pa"
//    println("Table " + tableName)
//
//    // create m7 table with column family
//    configuration.set(TableInputFormat.INPUT_TABLE, tableName)
//    configuration.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cfv")
//
//    val table = new HTable(configuration, tableName)
//
//    val sparkConf = new SparkConf().setAppName("a1").setMaster("local")
//    val sc = new SparkContext(sparkConf)
//
//    val rdd = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
//
//    return rdd
//  }

//  def doCount():Long = {
//
//    def a: ((ImmutableBytesWritable, Result)) => Geometry = {
//      s => new WKBReader().read(s._2.getValue("cfv".getBytes, "c1".getBytes))
//    }
//
//    val x :RDD[Geometry] = rdd.map(a)
//
//    for (a <- x) {
//      println("A: " + a.getGeometryType)
//    }
//
//    return rdd.count()
//  }
//}
