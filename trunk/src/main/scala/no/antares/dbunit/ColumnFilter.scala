/* ColumnFilter.scala
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

import org.dbunit.dataset.Column
import scala._
import collection.mutable.HashMap
import org.dbunit.dataset.filter.IColumnFilter

/**  */
class ColumnFilter extends IColumnFilter {
  private final val pks = new HashMap[String, Set[String]]

  def addTableJ( name: String, colNames: Array[String]) = addTable( name, colNames.toSeq: _* );
  def addTable( name: String, colNames: String*): Unit = {
    pks.put( name, colNames.toSet )
  }

  def accept(tableName: String, column: Column): Boolean = {
    pks.get( tableName ) match {
      case Some( e ) => return e.contains( column.getColumnName );
      case None => return false;
    }
  }
}
