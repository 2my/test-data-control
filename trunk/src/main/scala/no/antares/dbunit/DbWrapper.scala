/* DbWrapper.scala
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
import org.dbunit.dataset.CachedDataSet
import org.dbunit.operation.DatabaseOperation
import org.xml.sax.InputSource
import org.dbunit.dataset.xml.{FlatXmlDataSet, FlatXmlDataSetBuilder}
import xml.{XML, Node}
import org.dbunit.database.{ForwardOnlyResultSetTableFactory, DatabaseConfig, QueryDataSet, DatabaseConnection}
import org.dbunit.util.fileloader.FlatXmlDataFileLoader
import java.io._
import java.util.Properties
import java.sql.{DriverManager, Connection}
import org.apache.commons.io.IOUtils
import org.apache.tools.ant.taskdefs.SQLExec
import org.apache.tools.ant.Project
import org.codehaus.jettison.json.JSONObject
import io.{BufferedSource, Source}
import no.antares.util.FileUtil

/** Common Code for database
 * @author Tommy Skodje
*/
abstract class DbWrapper( val properties: DbProperties ) {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[DbWrapper] )

  loadDriver( properties.driver );
  protected val dbConnection: Connection	= connect()

  protected def connection(): Connection	= dbConnection;

  def getDbUnitConnection(): DatabaseConnection = {
    val dbuConnection = new DatabaseConnection( connection, properties.schema );
    val config = dbuConnection.getConfig();
    // config.setProperty( DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, true )
    config.setProperty( DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES, true )
    config.setProperty( DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new org.dbunit.ext.oracle.OracleDataTypeFactory() )
    dbuConnection
  }

  protected def connect(): Connection	= {
    val connectionUrl = properties.dbUrl;
    val connectionProperties = new Properties();
    connectionProperties.put( "user", properties.schema );
    connectionProperties.put( "username", properties.username );
    connectionProperties.put( "password", properties.password );

    DriverManager.getConnection( connectionUrl, connectionProperties )
  }

  /**  */
  def rollback(): Unit	= connection.rollback()

  /**  */
  def refreshWithFlatJSON( jsonS: String, convertCamelNames: Boolean ): Unit = {
    val json	= new JSONObject( jsonS )
    val producer  = new FlatJsonDataSetProducer( json )
    producer.setConvertCamelNames( convertCamelNames )
    val dataSet = new CachedDataSet( producer )
    DatabaseOperation.REFRESH.execute( getDbUnitConnection(), dataSet );
  }

  /**  */
  def refreshWithFlatJSON( json: File, convertCamelNames: Boolean ): Unit = {
    val lines = scala.io.Source.fromFile( json ).mkString
    refreshWithFlatJSON( lines, convertCamelNames );
  }

  /**  */
  def refreshWithFlatXml( xml: String ): Unit = {
    val builder = new FlatXmlDataSetBuilder();
    val is = new StringReader( xml );
    val dataSet = builder.build(new InputSource(is));

    DatabaseOperation.REFRESH.execute(getDbUnitConnection(), dataSet);
  }

  private def refreshWithFlatXmlFile( dbUnitFlatXmlFile: String ) {
    // problems locating File...
		// IDataSetProducer producer = new FlatXmlProducer( new InputSource( dbDataFile ) );
		// IDataSet dataSet = new StreamingDataSet(producer);
		val dataSet = ( new FlatXmlDataFileLoader() ).load( dbUnitFlatXmlFile );

		DatabaseOperation.REFRESH.execute( getDbUnitConnection(), dataSet );
  }

  /** partial database export */
  def extractFlatXml( tablesWithQueries: (String,String)* ): Node = {
    val partialDataSet = new QueryDataSet( getDbUnitConnection() );
    for ( (name, select) <- tablesWithQueries )
      partialDataSet.addTable( name, select );
    val partialResultW: StringWriter = new StringWriter();
    FlatXmlDataSet.write(partialDataSet, partialResultW);
    XML.loadString( partialResultW.toString )
  }

  /** full database export - setup for streaming @see http://www.dbunit.org/faq.html#streaming */
  def stream2FlatXml(): Node = {
    val dbuConnection = getDbUnitConnection();
    val config = dbuConnection.getConfig();
		config.setProperty( DatabaseConfig.PROPERTY_RESULTSET_TABLE_FACTORY, new ForwardOnlyResultSetTableFactory() );
		val fullDataSet = dbuConnection.createDataSet();
		val fullResultW	= new StringWriter();
		FlatXmlDataSet.write(fullDataSet, fullResultW );
		XML.loadString( fullResultW.toString )
  }

  /**  */
  def runSqlScriptFile( scriptFile: File ): Boolean = {
    logger.debug( "runSqlScriptFile: " + scriptFile );
    try {
      return runSqlScript( new FileInputStream(scriptFile) );
    } catch {
      case ex: FileNotFoundException => return false;
    }
  }

  def runSqlScripts( scripts: String* ): Boolean = {
    var ok  = true;
    scripts.foreach( script => ok = ok && runSqlScript( script ) )
    return ok;
  }

  /**  */
  def runSqlScript( script: String ): Boolean = {
    logger.debug( "runSqlScript: " + script );
    return runSqlScript( new ByteArrayInputStream( script.getBytes() ) );
  }

  /**  */
  def runSqlScript( script: Encoded[ InputStream ] ): Boolean = {
    try {
      connection.setAutoCommit(false);
      val result = runSqlScript( script, new Encoded[ OutputStream ]( System.out ) );
      connection.commit();
      return result;
    } finally {
      catchall { () => script.stream.close() };
    }
  }

  def runSqlScript( script: Encoded[ InputStream ] , output: Encoded[ OutputStream ] ): Boolean = {
    val writer = new StringWriter();
    IOUtils.copy( script.stream, writer, script.encoding )
    executeSql( writer.toString() )
    true
  }

  /** Use ant (http://stackoverflow.com/questions/2071682/how-to-execute-sql-script-file-in-java) */
  def executeSql( script: String ): Unit = {
      val executer = new SQLExec()
      val project = new Project();
      project.init();
      executer.setProject(project);
      executer.setTaskType("sql");
      executer.setTaskName("sql");

      // executer.setSrc(new File(sqlFilePath));
      executer.setDriver( properties.driver );
      executer.setPassword( properties.password );
      executer.setUserid( properties.username );
      executer.setUrl( properties.dbUrl );
      executer.addText( script )
      executer.execute();
  }

  /** Bundles stream with encoding, default to UTF-8 */
  class Encoded[T]( val stream: T, val encoding: String ) {
    def this( stream: T ) = this( stream, "UTF-8" )
  }
  /** Bundles stream with default encoding */
  implicit def stream2encoded( stream: InputStream ): Encoded[ InputStream ] = new Encoded[ InputStream ]( stream )
  implicit def stream2encoded( stream: OutputStream ): Encoded[ OutputStream ] = new Encoded[ OutputStream ]( stream )

  def catchall( f : () => Unit ): Unit = {
    try {
       f()
    } catch {
      case t: Throwable => ;	// ignore
    }
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
     *  In an embedded environment, any static Derby system properties
     *  must be set before loading the driver to take effect.
     */
    try {
      Class.forName(driver).newInstance();
      System.out.println("Loaded the appropriate driver");
    } catch {
      case ex: ClassNotFoundException => {
        System.err.println("\nUnable to load the JDBC driver " + driver);
        System.err.println("Please check your CLASSPATH.");
        ex.printStackTrace(System.err);
      }
      case ex: InstantiationException => {
        System.err.println(
          "\nUnable to instantiate the JDBC driver " + driver);
        ex.printStackTrace(System.err);
      }
      case ex: IllegalAccessException => {
        System.err.println(
          "\nNot allowed to access the JDBC driver " + driver);
        ex.printStackTrace(System.err);
      }
    }
  }

}