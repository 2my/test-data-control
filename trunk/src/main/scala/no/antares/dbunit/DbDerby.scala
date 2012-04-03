package no.antares.dbunit

/* DbDerbyala
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
import org.slf4j.{LoggerFactory, Logger}
import org.apache.derby.tools.ij
import org.dbunit.database.DatabaseConfig
import no.antares.util.Encoded
import java.io.{ByteArrayInputStream, OutputStream, InputStream}

/** Really for testing, but nice to have here for reference
 * @author tommy skodje
*/
class DbDerby extends DbProperties( "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:derbyDB;create=true", "TEST", "TEST", "TEST" ) {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[DbDerby] )

  dbUnitProperties.append( ( DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, false.asInstanceOf[Object] ) )

  override def runSqlScript( script: String ): Boolean = {
    try {
      val input  = new ByteArrayInputStream( script.getBytes() )
      val result  = ij.runScript( dbConnection, input, "UTF-8", System.out, "UTF-8" );
      logger.debug( "ij.runScript, result code is: " + result );
      return (result==0);
    } catch {
      case t: Throwable => return false;
    }
  }

}