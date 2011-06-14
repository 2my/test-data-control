/* FlatJsonDataSetProducer.scala
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

import java.util.ArrayList

import scala.collection.JavaConversions._
import collection.mutable.ListBuffer

import org.slf4j.{LoggerFactory, Logger}
import org.dbunit.dataset._
import stream.{BufferedConsumer, DefaultConsumer, IDataSetConsumer, IDataSetProducer}
import org.dbunit.dataset.datatype.DataType
import org.dbunit.dataset.xml._

import org.codehaus.jettison.json.{JSONArray, JSONObject}

/** Produces a dbUnit dataset from json, mirrors dbUnit FlatXmlProducer.

I chose to reimplement most of FlatXmlProducer here
I could have extended only produce() method in FlatXmlProducer, and use jettison XmlStreamReader
org.springframework.util.xml.StaxStreamXMLReader converts from XmlStreamReader to XmlReader

*/
class FlatJsonDataSetProducer(
  val json: JSONObject,
  var columnSensing: Boolean,    // dynamically recognize new columns during the parse process.
  var caseSensitiveTableNames: Boolean
) extends IDataSetProducer {

  private final val logger: Logger = LoggerFactory.getLogger(classOf[FlatXmlProducer])

  private var metaDataSet: IDataSet = null  // metadata for the tables, can be null

  private var consumer: IDataSetConsumer = new DefaultConsumer  // responsible for creating the datasets and tables
  private var _orderedTableNameMap: OrderedTableNameMap = null  // also holds the currently active {@link ITableMetaData}

  private def isNewTable(tableName: String): Boolean = { ! _orderedTableNameMap.isLastTable(tableName) }


  def this(json: JSONObject, metaDataSetIn: IDataSet) {
    this( json, false, metaDataSetIn.isCaseSensitiveTableNames )
    metaDataSet = metaDataSetIn
  }

  def this(json: JSONObject, columnSensing: Boolean) = this( json, columnSensing, false )

  def this( json: JSONObject ) =  this( json, true, false )

  /********************************************************************************/
  /** IDataSetProducer interface */
  def setConsumer( consumerIn: IDataSetConsumer): Unit = {
    logger.debug("setConsumer( consumerIn )")
    if ( columnSensing )
      consumer = new CamelNameConverterConsumer( new BufferedConsumer( consumerIn ) )
    else
      consumer = new CamelNameConverterConsumer( consumerIn )
  }

  /** IDataSetProducer interface */
  def produce: Unit = {
    // iterate json and call IDataSetConsumer functions
    logger.debug( "produce from " + json.toString( 2 ) )
    val dataset  = json.getJSONObject( "dataset" );
    consumer.startDataSet()
    _orderedTableNameMap = new OrderedTableNameMap( caseSensitiveTableNames )

    dataset.keys().foreach( key => produceTable( key.toString, dataset.getJSONArray( key.toString ) ) )
    consumer.endDataSet()
  }

  private def produceTable( name: String, rows: JSONArray ): Unit = {
    // [{ "colWithString":"1DEMOPARTNERK", "colWithInt":1, "colWithWhatever":null }, ...]
    logger.debug( "produceTable: " + name + "\n" + rows )

    if ( isNewTable( name ) ) {
      if ( _orderedTableNameMap.containsTable( name ) ) {
        _orderedTableNameMap.setLastTable( name )
      } else {
        val activeMetaData = createTableMetaData( name, Nil )
        _orderedTableNameMap.add(activeMetaData.getTableName, activeMetaData)
      }
    }

    val rowz  = produceRows( name, toList( rows ) )

    consumer.startTable( getActiveMetaData )
    for ( row <- rowz ) {
      consumer.row( alignRow2MetaData( row, getActiveMetaData ) )
    }
    consumer.endTable()
  }

  private def toList( rows: JSONArray ): List[ JSONObject ] = {
    val rowz  = new ListBuffer[ JSONObject ]
    for ( i <- 0.until( rows.length ) )
      rowz.append( rows.getJSONObject( i ) )
    rowz.toList
  }

  private def produceRows( tableName: String, rows: List[ JSONObject ] ): List[ ListBuffer[ NameValuePair ] ] = {
    if ( rows.isEmpty )
      return Nil;
    val row = produceRow( tableName, rows.head )
    row::produceRows( tableName, rows.tail )
  }

  private def produceRow( tableName: String, row: JSONObject ): ListBuffer[ NameValuePair ] = {
    println( "produceRow: " + row )

    val res = new ListBuffer[NameValuePair]
    row.keys().foreach( key => {
      val name  = key.toString
      res.append( new NameValuePair( name, row.get( name ) ) )
      val updates = findNewColumns(name, getActiveMetaData)
      updates.foreach( newMetaData => _orderedTableNameMap.update(newMetaData.getTableName, newMetaData) )
    } )

    res
  }

  private def findNewColumns(name: String, metaData: ITableMetaData): Option[ITableMetaData] = {
    try {
      metaData.getColumnIndex( name )
      return None
    } catch {
      case e: NoSuchColumnException => {
        // FixMe: logger.debug("Column sensing enabled. Will create a new metaData with potentially new columns if needed")
        val nCols = metaData.getColumns.length
        val columns = new Array[Column](nCols + 1)
        for (i <- 0.until(nCols))
          columns(i) = metaData.getColumns.apply(i)
        columns(nCols) = new Column(name, DataType.UNKNOWN)
        return Some( new DefaultTableMetaData(metaData.getTableName, columns) )
      }
    }
  }


  private def createTableMetaData(tableName: String, row: List[NameValuePair] ): ITableMetaData = {
    if (logger.isDebugEnabled)
      logger.debug("createTableMetaData(tableName={}, row={}) - start", tableName, row )
    if ( metaDataSet != null )
      return metaDataSet.getTableMetaData(tableName)

    var columns = row.map( column => new Column( column.name, DataType.UNKNOWN ) )
    return new DefaultTableMetaData(tableName, columns.toArray[ Column ] )
  }

  /** @return The currently active table metadata or <code>null</code> if no active metadata exists. */
  private def getActiveMetaData: ITableMetaData = {
    if (_orderedTableNameMap == null)
      return null
    val lastTableName: String = _orderedTableNameMap.getLastTableName
    if (lastTableName == null)
      return null
    return _orderedTableNameMap.get(lastTableName).asInstanceOf[ITableMetaData]
  }

  private def alignRow2MetaData( rowIn: ListBuffer[NameValuePair], metaData: ITableMetaData ): Array[Object] = {
    val nCols = metaData.getColumns.length
    val rowOut  = new ArrayList[Object]()
    for ( i <- 0.until( nCols ) )
      rowOut.add( null )
    rowIn.foreach( pair => { rowOut( metaData.getColumnIndex( pair.name ) )  = pair.value } )
    rowOut.toArray
  }

}

private class NameValuePair( val name: String, val value: AnyRef )
