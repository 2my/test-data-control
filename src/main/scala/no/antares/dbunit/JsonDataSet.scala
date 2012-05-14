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
import java.security.Key


/** @author Tommy Skodje */
class JsonDataSet(
  val jsonS: String,
  private val nameConverter: DefaultNameConverter
) {

  def this( jsonS: String )  = this( jsonS, new DefaultNameConverter() );
  def this( jsonF: File, nameConverter: DefaultNameConverter  = new DefaultNameConverter() )  = {
    this( scala.io.Source.fromFile( jsonF ).mkString, nameConverter )
  }

  def wrap()  = new JsonDataSet( """{"dataSet": ORIG}""".replace( "ORIG", jsonS ), nameConverter );

  private final val logger: Logger = LoggerFactory.getLogger(classOf[JsonDataSet])

  val json	= new JSONObject( jsonS )

  lazy val tables  = parseTables( json )

  def toString(indentFactor: Int): String = json.toString( indentFactor )

  private def parseTables( json: JSONObject ): List[ TableInDataSet ] = {
    val dataSets = parseDataSets( json )
    val tbls  = new TableMap()
    dataSets.foreach( o => tbls.putAll( tablesInDataSet( o ) ) );
    tbls.toList
  }

  private def parseDataSets( json: JSONObject ): List[ JSONObject ] = {
    val dataSets = new ListBuffer[ JSONObject ]()
    json.keys().foreach( key => {
      json.get( key.toString ) match {
        case a: JSONArray   => dataSets.addAll( toList( a ) )
        case o: JSONObject => dataSets.add( o )
        case other: AnyRef => {
          logger.error( "found unknown top-element in json data set: {}", other.toString )
          throw new RuntimeException( "found unknown top-element in json data set" )
        }
      }
    })
    dataSets.toList
  }

  private def tablesInDataSet( jsonO: JSONObject ): List[ TableInDataSet ] = {
    val tbls  = new ListBuffer[ TableInDataSet ]
    jsonO.keys().foreach( key => collect( key.toString ) )
    def collect( tName: String ): Unit = {
      if ( ! tName.isEmpty ) {
        jsonO.get( tName ) match {
          case a: JSONArray   => tbls.add( new TableInDataSet( tName, a, nameConverter ) )
          case o: JSONObject => tbls.add( new TableInDataSet( tName, toJSONArray( o ), nameConverter ) )
          case other: AnyRef => logger.error( "found unknown type for key ({}) in json data set: {}", tName, other.toString )
        }
      }
    }
    tbls.toList
  }

  private def toList( a: JSONArray ): List[ JSONObject ] = {
    val result  = new mutable.ListBuffer[ JSONObject ]
    for ( i <- 0.until( a.length ) ) {
      val o  = a.getJSONObject( i )
      result.append( o );
    }
    result.toList
  }

  private def toJSONArray( obj: JSONObject ) = ( new JSONArray() ).put( obj )

}

/** Keeps tables in map with name lookup to avoid multiple occurences of same table */
private class TableMap() {
  val tbls  = new mutable.HashMap[ String, TableInDataSet ]

  def toList(): List[ TableInDataSet ] = tbls.values.toList;

  def putAll( tables: List[ TableInDataSet ] ): Unit = tables.foreach( table => put( table ) );

  def put( table: TableInDataSet ): Unit = {
    if ( tbls.contains( table.tableName ) )
      tbls( table.tableName ).addRowsFrom( table )
    else
      tbls( table.tableName ) = table
  }
}

class TableInDataSet( val oldName: String, private val rowz: JSONArray, private val nameConverter: DefaultNameConverter ) {

  private final val logger: Logger = LoggerFactory.getLogger(classOf[TableInDataSet])

  val tableName  = nameConverter.tableName( oldName )
  lazy val metaData  = createTableMetaData()

  def addRowsFrom( copyFrom: TableInDataSet ): Unit = {
    val newRowz = copyFrom.rowz;
    for ( i <- 0.until( newRowz.length ) )
      rowz.put( newRowz.getJSONObject( i ) )
  }

  def rows(): Iterator[ RowInDataSet ] = {
    val result  = new mutable.ListBuffer[ RowInDataSet ]
    for ( i <- 0.until( rowz.length ) )
      result.append( new RowInDataSet( this, rowz.getJSONObject( i ) ) )
    result.toIterator
  }

  def row( index: Int ): List[ ColumnInDataSet ] = columns( rows().toList( index ) ).toList;

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

class ColumnInDataSet( val colName: String, val value: Object ) {
  override def toString: String = "(" + colName + ": " + value + ")";
}
