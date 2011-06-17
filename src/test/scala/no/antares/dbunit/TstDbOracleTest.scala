/* TstDbOracleTest.scala
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

import no.antares.dbunit.AbstractDB

/** @author Tommy Skodje */
class TstDbOracleTest extends AbstractDBTest {

  // FixMe: should not publish our database connection
  class TstDbOracle extends AbstractDB( "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:derbyDB;create=true", "TEST", "TEST", "TEST" ){}
  val db	= new TstDbOracle();

  // fails with heap space on our db
  override def verify_stream2FlatXml(): Unit = {}

}