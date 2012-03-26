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
import org.dbunit.database.{ForwardOnlyResultSetTableFactory, DatabaseConfig, QueryDataSet}
import org.dbunit.util.fileloader.FlatXmlDataFileLoader
import java.io._
import org.apache.commons.io.IOUtils
import org.dbunit.dataset.stream.DataSetProducerAdapter
import org.codehaus.jettison.json.JSONObject

/** Common Code for database
 * @author Tommy Skodje
*/
class DbWrapper( val db: Db ) {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[DbWrapper] )

  /**  */
  def refreshWithFlatJSON( json: JsonDataSet ): Unit = {
    val producer  = new FlatJsonDataSetProducer( json )
    val dataSet = new CachedDataSet( producer )
    DatabaseOperation.REFRESH.execute( getDbUnitConnection(), dataSet );
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

  def extractFlatXml( tablesWithQueries: (String,String)* ): Node = {
    val partialDataSet: QueryDataSet = dataSetFor(tablesWithQueries)
    val partialResultW: StringWriter = new StringWriter();
    FlatXmlDataSet.write(partialDataSet, partialResultW);
    XML.loadString( partialResultW.toString )
  }

  /** partial database export */
  def extractFlatJson( table: String, query: String ): JSONObject = extractFlatJson( (table, query) );
  def extractFlatJson( tablesWithQueries: Array[(String,String)] ): JSONObject = extractFlatJson(tablesWithQueries.toSeq : _*);
  def extractFlatJson( tablesWithQueries: (String,String)* ): JSONObject = {
    val partialDataSet: QueryDataSet = dataSetFor(tablesWithQueries)
    val partialResultW: StringWriter = new StringWriter();

    val strWriter = new StringWriter();
    val consumer  = new FlatJsonDataSetConsumer( strWriter )
    val provider: DataSetProducerAdapter = new DataSetProducerAdapter( partialDataSet )
    provider.setConsumer( consumer )
    provider.produce()

    new JSONObject( strWriter.toString )
  }

  /**  */
  private def dataSetFor(tablesWithQueries: Iterable[(String, String)] ): QueryDataSet = {
    val partialDataSet = new QueryDataSet(getDbUnitConnection());
    tablesWithQueries.foreach( e => partialDataSet.addTable( e._1, e._2) )
    // for ((name, select) <- tablesWithQueries) partialDataSet.addTable(name, select);
    partialDataSet
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

  def runSqlScripts( scripts: Array[String]) : Boolean = runSqlScripts(scripts.toSeq : _*);
  def runSqlScripts( scripts: String* ): Boolean = {
    var ok  = true;
    for ( script <- scripts )
      if ( ! runSqlScript( script ) )
        ok = false
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
      return doInTransaction{ () => runSqlScript( script, new Encoded[ OutputStream ]( System.out ) ); }
    } finally {
      catchall { () => script.stream.close() };
    }
  }


  def runSqlScript( script: Encoded[ InputStream ] , output: Encoded[ OutputStream ] ): Boolean = {
    val writer = new StringWriter();
    IOUtils.copy( script.stream, writer, script.encoding )
    db.executeSql( writer.toString() )
  }

  private def getDbUnitConnection() = db.getDbUnitConnection();

  def doInTransaction[T]( f : () => T ): T = {
    db.connection().setAutoCommit(false);
    val result = f();
    db.connection().commit();
    result
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

}