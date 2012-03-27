/* Dbscala
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

import converters.DefaultNameConverter
import org.slf4j.{LoggerFactory, Logger}
import collection.mutable.ListBuffer
import org.dbunit.database.{DatabaseConnection, DatabaseConfig}
import java.util.Properties
import java.sql.{DriverManager, Connection}
import java.io.OutputStream
import org.apache.tools.ant.taskdefs.SQLExec
import org.apache.tools.ant.Project

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

  private val dbUnitProperties  = new ListBuffer[ Tuple2[String, Object] ]();

  if ( driver.endsWith( "OracleDriver" ) ) {
    dbUnitProperties.append( ( DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES, true.asInstanceOf[Object] ) );
    dbUnitProperties.append( ( DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new org.dbunit.ext.oracle.OracleDataTypeFactory() ) );
  } else if ( driver.startsWith( "org.apache.derby" ) )
    dbUnitProperties.append( ( DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, false.asInstanceOf[Object] ) )
  else {
    dbUnitProperties.append( ( DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, false.asInstanceOf[Object] ) )
  }

  loadDriver( driver );
  protected val dbConnection: Connection	= connect()

  def connection(): Connection	= dbConnection;

  def getDbUnitConnection(): DatabaseConnection = {
    val dbuConnection =
      if ( schema.isEmpty )
        new DatabaseConnection( connection );
      else
        new DatabaseConnection( connection, schema );
    val config = dbuConnection.getConfig();
    dbUnitProperties.foreach( property => config.setProperty( property._1, property._2 ) );
    dbuConnection
  }

  /**  */
  def rollback(): Unit	= connection.rollback()

  /** Use ant (http://stackoverflow.com/questions/2071682/how-to-execute-sql-script-file-in-java) */
  def executeSql( script: String ): Boolean = {
    try {
      val executer = new SQLExec()
      val project = new Project();
      project.init();
      executer.setProject(project);
      executer.setTaskType("sql");
      executer.setTaskName("sql");

      // executer.setSrc(new File(sqlFilePath));
      executer.setDriver( driver );
      executer.setPassword( password );
      executer.setUserid( username );
      executer.setUrl( dbUrl );

      executer.addText( script )
      executer.execute();
    } catch {
      case t: Throwable => return false;
    }
    true;
  }

  protected def connect(): Connection	= {
    val connectionUrl = dbUrl;
    val connectionProperties = new Properties();
    connectionProperties.put( "user", username );
    connectionProperties.put( "username", username );
    connectionProperties.put( "password", password );

    DriverManager.getConnection( connectionUrl, connectionProperties )
  }

  /**
   * Loads the appropriate JDBC driver for this environment/framework. For
   * example, if we are in an embedded environment, we load Derby's
   * embedded Driver, <code>org.apache.derby.jdbc.EmbeddedDriver</code>.
   */
  protected def loadDriver( driver: String ): Unit = {
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