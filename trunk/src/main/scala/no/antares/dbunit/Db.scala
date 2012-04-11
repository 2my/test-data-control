/* Db.scala
   Copyright 2012 Tommy Skodje (http://www.antares.no)

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

import org.dbunit.database.IDatabaseConnection

/** Abstract superclass for interfacing different databases.
 * @author tommy skodje
*/
abstract class Db {

  /** @return database connection wrapper for dbUnit, implementor must override */
  def getDbUnitConnection(): IDatabaseConnection;

  /** For easy access from java */
  def runSqlScripts( scripts: Array[String]) : Array[Boolean] = runSqlScripts(scripts.toSeq : _*).toArray;
  /** Runs all scripts, @return status for each script */
  def runSqlScripts( scripts: String* ): Seq[Boolean] = scripts.map( runSqlScript( _ ) );

  /** run script (ddl) an @return status, implementor must override */
  def runSqlScript( script: String ): Boolean;

  /** rollback after test */
  def rollback(): Unit = getDbUnitConnection().getConnection().rollback();

  /** utility for subclasses */
  protected def doInTransaction[T]( f : () => T ): T = {
    val con = getDbUnitConnection().getConnection()
    con.setAutoCommit(false);
    val result = f();
    con.commit();
    result
  }

}