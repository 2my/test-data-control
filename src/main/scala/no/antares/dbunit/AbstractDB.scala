/* AbstractDB.scala
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
import java.sql.Connection
import org.codehaus.jettison.json.JSONObject
import org.dbunit.dataset.CachedDataSet
import org.dbunit.operation.DatabaseOperation
import org.xml.sax.InputSource
import org.dbunit.dataset.xml.{FlatXmlDataSet, FlatXmlDataSetBuilder}
import xml.{XML, Node}
import org.dbunit.database.{ForwardOnlyResultSetTableFactory, DatabaseConfig, QueryDataSet, DatabaseConnection}
import java.io._

/** Common Code for database
 * @author Tommy Skodje
*/
abstract class AbstractDB {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[AbstractDB] )

  /** must be overriden */
  def connection(): Connection;

  /** must be overriden */
  def connect(): Connection;

  /** must be overriden */
  def scriptRunner( script: Encoded[ InputStream ] , output: Encoded[ OutputStream ] ): Boolean;

  /** Bundles stream with encoding, default to UTF-8 */
  class Encoded[T]( val stream: T, val encoding: String ) {
    def this( stream: T ) = this( stream, "UTF-8" )
  }
  /** Bundles stream with default encoding */
  implicit def stream2encoded( stream: InputStream ): Encoded[ InputStream ] = new Encoded[ InputStream ]( stream )
  implicit def stream2encoded( stream: OutputStream ): Encoded[ OutputStream ] = new Encoded[ OutputStream ]( stream )

  /**  */
  def rollback(): Unit	= connection.rollback()

  /**  */
  def refreshWithFlatJSON( jsonS: String ): Unit = {
    val json	= new JSONObject( jsonS )
    val dataSet = new CachedDataSet( new FlatJsonDataSetProducer( json ) )
    val dbuConnection = new DatabaseConnection(connection);
    DatabaseOperation.REFRESH.execute( dbuConnection, dataSet );
  }

  /**  */
  def refreshWithFlatXml( xml: String ): Unit = {
    val builder = new FlatXmlDataSetBuilder();
    val is = new StringReader( xml );
    val dataSet = builder.build(new InputSource(is));

    val dbuConnection = new DatabaseConnection(connection);
    DatabaseOperation.REFRESH.execute(dbuConnection, dataSet);
  }

  /** partial database export */
  def extractFlatXml( tablesWithQueries: (String,String)* ): Node = {
    val dbuConnection = new DatabaseConnection( connection );
    val partialDataSet = new QueryDataSet(dbuConnection);
    for ( (name, select) <- tablesWithQueries )
      partialDataSet.addTable( name, select );
    val partialResultW: StringWriter = new StringWriter();
    FlatXmlDataSet.write(partialDataSet, partialResultW);
    XML.loadString( partialResultW.toString )
  }

  /** full database export - setup for streaming @see http://www.dbunit.org/faq.html#streaming */
  def stream2FlatXml(): Node = {
    val dbuConnection = new DatabaseConnection( connection );
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

  /**  */
  def runSqlScript( script: String ): Boolean = {
    logger.debug( "runSqlScript: " + script );
    return runSqlScript( new ByteArrayInputStream( script.getBytes() ) );
  }

  /**  */
  def runSqlScript( script: Encoded[ InputStream ] ): Boolean = {
    try {
      connection.setAutoCommit(false);
      val result = scriptRunner( script, new Encoded[ OutputStream ]( System.out ) );
      connection.commit();
      return result;
    } finally {
      catchall { () => script.stream.close() };
    }
  }

  def catchall( f : () => Unit ): Unit = {
    try {
       f()
    } catch {
      case t: Throwable => ;	// ignore
    }
  }

}