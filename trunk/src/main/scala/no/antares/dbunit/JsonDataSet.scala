/* JsonDataSet.scala
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

import org.dbunit.dataset.datatype.DataType
import org.slf4j.{LoggerFactory, Logger}
import org.dbunit.dataset._
import org.codehaus.jettison.json.{JSONObject, JSONArray}

import scala.collection.JavaConversions._
import scala.collection._

/** @author Sommy Skodje */
class JsonDataSet( val jsonS: String ) {
  private final val logger: Logger = LoggerFactory.getLogger(classOf[JsonDataSet])

  val json	= new JSONObject( jsonS )
  val dataset  = json.getJSONObject( "dataset" );

  def tables(): Iterator[ TableInDataSet ] = {
    dataset.keys().map( key => new TableInDataSet( key.toString, dataset.getJSONArray( key.toString ) ) )
  }

  def toString(indentFactor: Int): String = json.toString( indentFactor )
}

class TableInDataSet( val name: String, private val rowz: JSONArray ) {
  private final val logger: Logger = LoggerFactory.getLogger(classOf[TableInDataSet])

  lazy val metaData  = createTableMetaData()

  def rows(): Iterator[ RowInDataSet ] = {
    val result  = new mutable.ListBuffer[ RowInDataSet ]
    for ( i <- 0.until( rowz.length ) )
      result.append( new RowInDataSet( this, rowz.getJSONObject( i ) ) )
    result.toIterator
  }

  def columns( row: RowInDataSet ): Iterator[ ColumnInDataSet ] = {
    val columnz  = new mutable.ListBuffer[ ColumnInDataSet ]

    row.rowO.keys().foreach( key => {
      val name  = key.toString
      columnz.append( new ColumnInDataSet( name, row.rowO.get( name ) ) )
    } )
    columnz.toIterator
  }

  def align2MetaData( rowIn: RowInDataSet ): Array[Object] = {
    val nCols = metaData.getColumns.length
    val rowOut  = new ArrayList[Object]()
    for ( i <- 0.until( nCols ) )
      rowOut.add( null )
    columns( rowIn ).foreach( column => { rowOut( metaData.getColumnIndex( column.name ) )  = column.value } )
    rowOut.toArray
  }

  private def createTableMetaData(): ITableMetaData = {
    if (logger.isDebugEnabled)
      logger.debug("createTableMetaData(tableName={}) - start", name )
    val cols  = new mutable.HashSet[ String ]()
    rows().foreach( row => columns( row ).foreach( column => cols.add( column.name ) ) )
    val columnz = cols.map( colName => new Column( colName, DataType.UNKNOWN ) )
    return new DefaultTableMetaData( name, columnz.toArray[ Column ] )
  }

}

class RowInDataSet( val table: TableInDataSet, val rowO: JSONObject ) {
  def align2MetaData(): Array[Object] = {
    table.align2MetaData( this )
  }
}

class ColumnInDataSet( val name: String, val value: Object )
