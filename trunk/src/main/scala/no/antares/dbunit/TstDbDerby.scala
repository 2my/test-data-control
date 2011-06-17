/* TstDbDerby.scala
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

import java.io._

import org.apache.derby.tools.ij;

import org.slf4j.{LoggerFactory, Logger}

/** Implements AbstractDB for Derby (In-memory database)
 *
 * @author Tommy Skodje
 */
class TstDbDerby extends AbstractDB( "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:derbyDB;create=true", "TEST", "TEST", "TEST" ) {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[TstDbDerby] )

  override def runSqlScript( script: Encoded[ InputStream ] , output: Encoded[ OutputStream ] ): Boolean = {
    val result = ij.runScript( dbConnection, script.stream, script.encoding, output.stream, output.encoding );
    logger.debug( "ij.runScript, result code is: " + result );
    return (result==0);
  }

}
