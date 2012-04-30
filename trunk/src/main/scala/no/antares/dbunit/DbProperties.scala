/* DbProperties.scala
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

import org.slf4j.{LoggerFactory, Logger}
import java.sql.{DriverManager, Connection}
import org.dbunit.database.{IDatabaseConnection, DatabaseConnection}
import java.util.Properties
import collection.mutable.ListBuffer

/** Simple wrapper for Database connection db.
@author Tommy Skodje
*/
class DbProperties(
  val driver: String,
  val dbUrl: String,
  val username: String,
  val password: String,
  val schema: String
) extends Db {

  def this( driver: String, dbUrl: String, username: String, password: String )  = this( driver, dbUrl, username, password, "" );


  private final val logger: Logger = LoggerFactory.getLogger( classOf[DbWrapper] )
  loadDriver( driver );
  val connectionProperties = new Properties();
  connectionProperties.put( "user", username );
  connectionProperties.put( "username", username );
  connectionProperties.put( "password", password );
  protected val dbConnection: Connection	= DriverManager.getConnection( dbUrl, connectionProperties );



  override def runSqlScript( script: String ): Boolean = {
    val runner = new ScriptRunner( driver, dbUrl, username, password );
    doInTransaction { () => runner.executeSql( script ) }
  }

  override def getDbUnitConnection(): IDatabaseConnection = {
    val dbuConnection =
      if ( schema.isEmpty )
        new DatabaseConnection( dbConnection );
      else
        new DatabaseConnection( dbConnection, schema );
    val config = dbuConnection.getConfig();
    dbUnitProperties.foreach( property => config.setProperty( property._1, property._2 ) );
    dbuConnection
  }


  /**
   * Loads the appropriate JDBC driver for this environment/framework. For
   * example, if we are in an embedded environment, we load Derby's
   * embedded Driver, <code>org.apache.derby.jdbc.EmbeddedDriver</code>.
   */
  private def loadDriver( driver: String ): Unit = {
    /*
     *  The JDBC driver is loaded by loading its class.
     *  If you are using JDBC 4.0 (Java SE 6) or newer, JDBC drivers may
     *  be automatically loaded, making this code optional.
     *
     *  In an embedded environment, this will also start up the Derby
     *  engine (though not any databases), since it is not already
     *  running. In a client environment, the Derby engine is being run
     *  by the network server framework.
     *
     *  In an embedded environment, any static Derby system db
     *  must be set before loading the driver to take effect.
     */
    try {
      Class.forName(driver).newInstance();
      logger.debug( "Loaded the appropriate driver" );
    } catch {
      case ex: ClassNotFoundException => {
        logger.error( "\nUnable to load the JDBC driver " + driver + "\nPlease check your CLASSPATH." , ex );
      }
      case ex: InstantiationException => {
        logger.error( "\nUnable to instantiate the JDBC driver " + driver, ex );
      }
      case ex: IllegalAccessException => {
        logger.error( "\nNot allowed to access the JDBC driver " + driver, ex );
      }
    }
  }

}