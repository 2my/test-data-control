/* TstContext.scala
   Copyright 2011 Tommy Skodje (http://www.antares.no)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package no.antares.utils

import org.apache.commons.digester.Digester
import no.antares.dbunit.DbProperties
import collection.mutable.HashMap
import java.io.File

/**
 * @author Tommy Skodje
 */
class TstContext( val dataFile: String ) {
  val dataBases = new HashMap[String, DbProperties]()

  val digester: Digester = new Digester

  digester.push(this)
  digester.addCallMethod("context/datasource", "addDataSource", 6)
  digester.addCallParam("context/datasource/name", 0)
  digester.addCallParam("context/datasource/driver", 1)
  digester.addCallParam("context/datasource/url", 2)
  digester.addCallParam("context/datasource/username", 3)
  digester.addCallParam("context/datasource/password", 4)
  digester.addCallParam("context/datasource/schema", 5)
  val fUrl  = getClass.getClassLoader.getResource( dataFile )
  if ( fUrl != null )
    digester.parse( fUrl )

  def addDataSource(name: String, driver: String, url: String, userName: String, password: String, schema: String): Unit = {
    dataBases.put( name, new DbProperties( driver, url, userName, password, schema ) )
  }

  def dataSources(): Map[String, DbProperties] = dataBases.toMap
  def dataSourceDerby(): Option[DbProperties] = dataBases.get( "Derby" )
  def dataSourceOracle(): Option[DbProperties] = dataBases.get( "Oracle" )

}

