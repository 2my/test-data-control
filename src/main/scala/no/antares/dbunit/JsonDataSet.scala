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
import java.io.File
import mutable.ListBuffer

import converters.DefaultNameConverter
import java.lang.RuntimeException


/** @author Tommy Skodje */
class JsonDataSet(
  val jsonS: String,
  private val nameConverter: DefaultNameConverter
) {
  def this( jsonS: String )  = this( jsonS, new DefaultNameConverter() );
  def this( jsonF: File, nameConverter: DefaultNameConverter  = new DefaultNameConverter() )  = {
    this( scala.io.Source.fromFile( jsonF ).mkString, nameConverter )
  }


  private final val logger: Logger = LoggerFactory.getLogger(classOf[JsonDataSet])

  val json	= new JSONObject( jsonS )
  val dataset: JSONArray = json.get( nameConverter.dataSetName() ) match {
    case a: JSONArray   => a
    case o: JSONObject => toJSONArray( o )
    case other: AnyRef => {
      logger.error( "found unknown top-element in json data set: {}", other.toString )
      throw new RuntimeException( "found unknown top-element in json data set" )
    }
  }


  def tables(): List[ TableInDataSet ] = {
    val tbls  = new ListBuffer[ TableInDataSet ]
    foreach( dataset, collect(_) )
    def collect( o: JSONObject ): Unit = tbls.appendAll( tablesO( o ) )

    tbls.toList
  }

  def toString(indentFactor: Int): String = json.toString( indentFactor )

  private def tablesO( o: JSONObject ): List[ TableInDataSet ] = {
    val tbls  = new ListBuffer[ TableInDataSet ]
    o.keys().foreach( key => collect( key.toString ) )
    def collect( tName: String ): Unit = {
      if ( ! tName.isEmpty ) {
        o.get( tName ) match {
          case a: JSONArray   => tbls.add( new TableInDataSet( tName, a, nameConverter ) )
          case o: JSONObject => tbls.add( new TableInDataSet( tName, toJSONArray( o ), nameConverter ) )
          case other: AnyRef => logger.error( "found unknown type for key ({}) in json data set: {}", tName, other.toString )
        }
      }
    }

    tbls.toList
  }

  private def toJSONArray( obj: JSONObject ) = ( new JSONArray() ).put( obj )

  private def foreach[T]( a: JSONArray, f: JSONObject => T ): List[ T ] = {
    val result  = new mutable.ListBuffer[ T ]
    for ( i <- 0.until( a.length ) ) {
      val o  = a.getJSONObject( i )
      result.append( f( o ) );
    }
    result.toList
  }

}

class TableInDataSet( val oldName: String, private val rowz: JSONArray, private val nameConverter: DefaultNameConverter ) {

  private final val logger: Logger = LoggerFactory.getLogger(classOf[TableInDataSet])

  val tableName  = nameConverter.tableName( oldName )
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
      val value  = row.rowO.get( key.toString )
      val colName  = nameConverter.columnName( key.toString )
      columnz.append( new ColumnInDataSet( colName, value ) )
    } )
    columnz.toIterator
  }

  def align2MetaData( rowIn: RowInDataSet ): Array[Object] = {
    val nCols = metaData.getColumns.length
    val rowOut  = new ArrayList[Object]()
    for ( i <- 0.until( nCols ) )
      rowOut.add( null )
    columns( rowIn ).foreach( column => { rowOut( metaData.getColumnIndex( column.colName ) )  = column.value } )
    rowOut.toArray
  }

  private def createTableMetaData(): ITableMetaData = {
    if (logger.isDebugEnabled)
      logger.debug("createTableMetaData(tableName={}) - start", tableName )
    val cols  = new mutable.HashSet[ String ]()
    rows().foreach( row => columns( row ).foreach( column => cols.add( column.colName ) ) )
    val columnz = cols.map( colName => new Column( colName, DataType.UNKNOWN ) )
    return new DefaultTableMetaData( tableName, columnz.toArray[ Column ] )
  }

}

class RowInDataSet( val table: TableInDataSet, val rowO: JSONObject ) {
  def align2MetaData(): Array[Object] = {
    table.align2MetaData( this )
  }
}

class ColumnInDataSet( val colName: String, val value: Object )
