/* DbDataSource.scala
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
package no.antares.dbunit

import org.dbunit.database.{DatabaseDataSourceConnection, IDatabaseConnection}
import collection.mutable.ListBuffer
import org.springframework.jdbc.datasource.DriverManagerDataSource
import javax.sql.DataSource

/**
 * @author tommy skodje
*/
class DbDataSource(
//  val ds: DataSource, // FixMe: should not have to provide line below
  val driver: String, val dbUrl: String, val username: String, val password: String,
  val schema: String
) extends Db {

  val ds  = DbDataSource.getDataSource( driver, dbUrl, username, password );

  protected val dbUnitProperties  = new ListBuffer[ Tuple2[String, Object] ]();

  override def rollback(): Unit = ds.getConnection().rollback();

  override def getDbUnitConnection(): IDatabaseConnection = {
    val dbuConnection =
      if ( schema.isEmpty )
        new DatabaseDataSourceConnection( ds );
      else
        new DatabaseDataSourceConnection( ds, schema );
    val config = dbuConnection.getConfig();
    dbUnitProperties.foreach( property => config.setProperty( property._1, property._2 ) );
    dbuConnection
  }

  override def runSqlScript( script: String ): Boolean = {
    val dbs = new ScriptRunner( driver, dbUrl, username, password );
    doInTransaction { () => dbs.executeSql( script ) }
   }

}

object DbDataSource {
  val instance = new DriverManagerDataSource();

  def getDataSource( driverClass: String, dbUrl: String, username: String, password: String ): DataSource = {
    instance.setDriverClassName( driverClass );
    instance.setUrl( dbUrl );
    instance.setUsername( username );
    instance.setPassword( password );
    return instance;
  }
}