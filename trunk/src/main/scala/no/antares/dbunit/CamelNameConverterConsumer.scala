/* CamelNameConverterConsumer.scala
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

import org.dbunit.dataset.stream.IDataSetConsumer
import org.dbunit.dataset.{Column, ITableMetaData}
import scala.Array
import java.lang.StringBuilder
import collection.mutable.HashMap

/**  */
private class CamelNameConverterConsumer( val wrappedConsumer: IDataSetConsumer ) extends IDataSetConsumer {

  def startDataSet: Unit  = wrappedConsumer.startDataSet();

  def endDataSet: Unit    = wrappedConsumer.endDataSet();

  def startTable(metaData: ITableMetaData): Unit  = {
    wrappedConsumer.startTable( new CamelNameConverter( metaData ) )
  }

  def endTable: Unit  = wrappedConsumer.endDataSet();

  def row(values: Array[AnyRef]): Unit  = wrappedConsumer.row( values );
}

private class CamelNameConverter( val wrapped: ITableMetaData ) extends ITableMetaData {
  private val tableName = camel2underscored( wrapped.getTableName );
  private val columns  = camel2underscored( wrapped.getColumns );
  private val pks  = camel2underscored( wrapped.getPrimaryKeys );
  private val indices  = new HashMap[String,Int]()
  for ( i <- 0.until( columns.length ) )
    indices.put( columns(i).getColumnName, i )

  def getTableName: String = {
    tableName
  }
  def getColumns: Array[Column] = {
    columns
  }
  def getPrimaryKeys: Array[Column] = {
    pks
  }
  def getColumnIndex( columnName: String ): Int = {
    // println( "getColumnIndex for " + columnName ) // ? We may be asked for CamelCased names, should check
    indices( columnName )
  }

  private def camel2underscored( name: String ): String = {
    val underscored = new StringBuilder()
    for ( c <- name.toArray[Char] ) {
      if ( c.isLower || ( underscored.length() == 0 ) )
        underscored.append( c.toUpper )
      else
        underscored.append( "_" ).append( c )
    }
    underscored.toString
  }
  private def camel2underscored( columnsIn: Array[Column] ): Array[Column] = {
    columnsIn.map( column => camel2underscored( column ) )
  }
  private def camel2underscored( columnIn: Column ): Column = {
    new Column(
      camel2underscored( columnIn.getColumnName), columnIn.getDataType, columnIn.getSqlTypeName,
      columnIn.getNullable, columnIn.getDefaultValue, columnIn.getRemarks, columnIn.getAutoIncrement
    )
  }
}


