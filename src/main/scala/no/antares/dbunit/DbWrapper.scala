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
import org.dbunit.dataset.stream.DataSetProducerAdapter
import org.codehaus.jettison.json.JSONObject

/** Common Code for database
 * @author Tommy Skodje
*/
class DbWrapper( val db: Db ) {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[DbWrapper] )

  val dbUnitConnection = db.getDbUnitConnection();

  /**  */
  def refreshWithFlatJSON( json: JsonDataSet ): Unit = {
    val producer  = new FlatJsonDataSetProducer( json )
    val dataSet = new CachedDataSet( producer )
    DatabaseOperation.REFRESH.execute( dbUnitConnection, dataSet );
  }

  /**  */
  def deleteMatchingFlatJSON( json: JsonDataSet ): Unit = {
    val producer  = new FlatJsonDataSetProducer( json )
    val dataSet = new CachedDataSet( producer )
    DatabaseOperation.DELETE.execute( dbUnitConnection, dataSet );
  }

  /**  */
  def refreshWithFlatXml( xml: String ): Unit = {
    val builder = new FlatXmlDataSetBuilder();
    val is = new StringReader( xml );
    val dataSet = builder.build(new InputSource(is));

    DatabaseOperation.REFRESH.execute( dbUnitConnection, dataSet);
  }

  private def refreshWithFlatXmlFile( dbUnitFlatXmlFile: String ) {
    // problems locating File...
		// IDataSetProducer producer = new FlatXmlProducer( new InputSource( dbDataFile ) );
		// IDataSet dataSet = new StreamingDataSet(producer);
		val dataSet = ( new FlatXmlDataFileLoader() ).load( dbUnitFlatXmlFile );

		DatabaseOperation.REFRESH.execute( dbUnitConnection, dataSet );
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
  private def dataSetFor( tablesWithQueries: Iterable[(String, String)] ): QueryDataSet = {
    val partialDataSet = new QueryDataSet(dbUnitConnection);
    tablesWithQueries.foreach( e => partialDataSet.addTable( e._1, e._2) )
    // for ((name, select) <- tablesWithQueries) partialDataSet.addTable(name, select);
    partialDataSet
  }

  /** full database export - setup for streaming @see http://www.dbunit.org/faq.html#streaming */
  def stream2FlatXml(): Node = {
    val dbuConnection = dbUnitConnection;
    val config = dbuConnection.getConfig();
		config.setProperty( DatabaseConfig.PROPERTY_RESULTSET_TABLE_FACTORY, new ForwardOnlyResultSetTableFactory() );
		val fullDataSet = dbuConnection.createDataSet();
		val fullResultW	= new StringWriter();
		FlatXmlDataSet.write(fullDataSet, fullResultW );
		XML.loadString( fullResultW.toString )
  }

}