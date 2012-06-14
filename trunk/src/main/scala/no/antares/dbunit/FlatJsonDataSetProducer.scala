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

import org.slf4j.{LoggerFactory, Logger}
import org.dbunit.dataset._
import stream.{BufferedConsumer, DefaultConsumer, IDataSetConsumer, IDataSetProducer}
import org.dbunit.dataset.xml._

/** Produces a dbUnit dataset from json, mirrors dbUnit FlatXmlProducer.

I chose to reimplement most of FlatXmlProducer here
I could have extended only produce() method in FlatXmlProducer, and use jettison XmlStreamReader
org.springframework.util.xml.StaxStreamXMLReader converts from XmlStreamReader to XmlReader

*/
class FlatJsonDataSetProducer(
  val json: JsonDataSet,
  var columnSensing: Boolean,    // dynamically recognize new columns during the parse process.
  var caseSensitiveTableNames: Boolean
) extends IDataSetProducer {

  def this( json: JsonDataSet ) =  this( json, true, false )

  private val logger: Logger = LoggerFactory.getLogger(classOf[FlatXmlProducer])
  private var consumer: IDataSetConsumer = new DefaultConsumer  // responsible for creating the datasets and tables
  private var _orderedTableNameMap: OrderedTableNameMap = null  // also holds the currently active {@link ITableMetaData}

  private def isNewTable(tableName: String): Boolean = { ! _orderedTableNameMap.isLastTable(tableName) }

  /********************************************************************************/
  /** IDataSetProducer interface */
  def setConsumer( consumerIn: IDataSetConsumer): Unit = {
    logger.debug("setConsumer( consumerIn )")
    if ( columnSensing )
      consumer = new BufferedConsumer( consumerIn )
    else
      consumer = consumerIn
  }

  /** IDataSetProducer interface */
  def produce: Unit = {
    // iterate json and call IDataSetConsumer functions
    logger.debug( "produce from " + json.toString( 2 ) )
    consumer.startDataSet()
    _orderedTableNameMap = new OrderedTableNameMap( caseSensitiveTableNames )

    json.tables.foreach( produceTable( _ ) )
    consumer.endDataSet()
  }

  private def produceTable( table: TableInDataSet ): Unit = {
    // [{ "colWithString":"1DEMOPARTNERK", "colWithInt":1, "colWithWhatever":null }, ...]
    logger.debug( "produceTable: " + table.tableName )

    if ( isNewTable( table.tableName ) ) {
      if ( _orderedTableNameMap.containsTable( table.tableName ) )
        _orderedTableNameMap.setLastTable( table.tableName )
      else
        _orderedTableNameMap.add(table.tableName, createTableMetaData( table ) )
    }

    consumer.startTable( table.metaData )
    table.rows().foreach( row => consumer.row( row.align2MetaData() ) )
    consumer.endTable()
  }

  def createTableMetaData( table: TableInDataSet ): ITableMetaData = table.metaData


/*
  def this(json: JsonDataSet, metaDataSetIn: IDataSet) {
    this( json, false, metaDataSetIn.isCaseSensitiveTableNames )
    metaDataSet = metaDataSetIn
  }

  def this(json: JsonDataSet, columnSensing: Boolean) = this( json, columnSensing, false )

  private var metaDataSet: IDataSet = null  // metadata for the tables, can be null
  def createTableMetaData( table: TableInDataSet ): ITableMetaData = {
    if ( metaDataSet != null )
      metaDataSet.getTableMetaData( table.name )
    else
      table.metaData
  }
*/

}

