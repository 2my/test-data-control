/* DbTest.scala
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

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.scalatest.junit.AssertionsForJUnit
import org.junit.runners.Parameterized.Parameters
import java.util.ArrayList
import org.slf4j.{LoggerFactory, Logger}
import org.junit.{Test, After}

/**
 * @author tommy skodje
*/
@RunWith(classOf[Parameterized])
class DbTest( val dbp: Db ) extends AssertionsForJUnit {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[DbTest] )

  // @After def cleanUp  = dbp.rollback()

  @Test def getDbUnitConnection() {

  }

  @Test def runSqlScript() {

  }

  @Test def rollback() {

  }

}

// TODO: use FunSuite http://www.scalatest.org/scaladoc-1.6.1/#org.scalatest.FunSuite
// see: http://jpz-log.info/archives/2009/09/29/scalatest-in-maven/
object DbTest {
  @Parameters
  def configurationsPresent(): ArrayList[ Array[Object] ] = {
    val configurations	= new ArrayList[ Array[Object] ]

    // Test with in-memory DB, default.
    configurations.add( Array( new DbDerby() ) )
    configurations.add( Array( new DbDataSource( "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:derbyDB;create=true", "TEST", "TEST" ) ) )
    // configurations.add( Array( new TstDbOracle() ) )

    return configurations
  }
}
