package io.hgis

/**
 * Created by tempehu on 24-Apr-15.
 */



import java.io.File

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Response
import javax.ws.rs.{GET, Path, DefaultValue, QueryParam, WebApplicationException}
import javax.ws.rs.core.{Response, Context}

import com.typesafe.config.ConfigFactory



@Path("/wms")
class WMSResource {

  @GET
  def get(
           // WMS request parameters
           @DefaultValue("1")
           @QueryParam("VERSION")
           version:String,

           @DefaultValue("GetCapabilities")
           @QueryParam("REQUEST")
           request:String,

           @DefaultValue("")
           @QueryParam("STYLES")
           styles:String,

           @DefaultValue("-8379782.57151,4846436.32082,-8360582.57151,4865636.32082")
           @QueryParam("BBOX")
           bbox:String,

           @DefaultValue("256")
           @QueryParam("WIDTH")
           cols:String,

           @DefaultValue("256")
           @QueryParam("HEIGHT")
           rows:String,

           @DefaultValue("")
           @QueryParam("LAYERS")
           layers:String,

           @DefaultValue("info")
           @QueryParam("FORMAT")
           format:String,

           @DefaultValue("true")
           @QueryParam("TRANSPARENT")
           transparent:String,

           @DefaultValue("")
           @QueryParam("SRS")
           srs:String,

           @DefaultValue("")
           @QueryParam("BGCOLOR")
           bgcolor:String,

           // Custom parameters
           @DefaultValue("ff0000,ffff00,00ff00,0000ff")
           @QueryParam("palette")
           palettex:String,

           @DefaultValue("http://192.168.16.41:8888/wms")
           @QueryParam("url")
           url:String,

           @Context req:HttpServletRequest
           ) = {

    val query = req.getQueryString()
    println(query)

    if (request != "GetMap") {

//      Response.ok(xml.toString).`type`("text/xml").build()

    } else {
      // Create operations for WMS operation.  Note that we do not run these operations
      // until server.run is called.

      /**
       * First, let's figure out what geographical area we're interested in, as
       * well as the resolution we want to use.
       */
    }
  }
}


object WMSResource {

  // Information page for debugging.
  def infoPage(cols:Int, rows:Int, ms:Long, url:String, tree:String) = """
<html>
<head>
 <script type="text/javascript">
 </script>
</head>
<body>
 <h2>WMS service template</h2>
 <h3>rendered %dx%d image (%d pixels) in %d ms</h3>
 <table>
  <tr>
   <td style="vertical-align:top"><img style="vertical-align:top" src="%s" /></td>
   <td><pre>%s</pre></td>
  </tr>
 </table>
</body>
</html>
                                                                       """ format(cols, rows, cols * rows, ms, url, tree)
}