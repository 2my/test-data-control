package no.antares.dbunit.converters

/* DefaultNameConverter.scala
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
import org.dbunit.dataset.Column

/** Interface + default implementation of a name converter
 @author Tommy Skodje
*/
class DefaultNameConverter() {

  def tableName( oldName: String ): String  = oldName;
  def columnName( oldName: String ): String  = oldName;

  private def convertColumns( columnsIn: Array[Column] ): Array[Column] = {
    columnsIn.map( column => convertColumn( column ) )
  }
  private def convertColumn( columnIn: Column ): Column = {
    new Column(
      columnName( columnIn.getColumnName ), columnIn.getDataType, columnIn.getSqlTypeName,
      columnIn.getNullable, columnIn.getDefaultValue, columnIn.getRemarks, columnIn.getAutoIncrement
    )
  }
}