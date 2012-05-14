package no.antares.dbunit.converters

/* AlphaRemoverTest.scala
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
import org.scalatest.junit.AssertionsForJUnit
import org.junit._
import org.junit.Assert._
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers._


/**  @author Tommy Skodje */
class AlphaRemoverTest  extends AssertionsForJUnit {

  @Test def testTableName() = {
    var converter  = new AlphaRemover()
    assertThat( converter.tableName( "@TABLE_NAME_KEPT" ), equalTo( "@TABLE_NAME_KEPT" ) )
    converter  = new AlphaRemover( new CamelNameConverter() )
    assertThat( converter.tableName( "@underscoredIsForwarded" ), equalTo( "@UNDERSCORED_IS_FORWARDED" ) )
  }

  @Test def testColumnName() = {
    var converter  = new AlphaRemover()
    assertThat( converter.columnName( "@COLUMN_NAME_CLEANED" ), equalTo( "COLUMN_NAME_CLEANED" ) )
    assertThat( converter.columnName( "COLUMN_NAME_UNTOUCHED" ), equalTo( "COLUMN_NAME_UNTOUCHED" ) )

    converter  = new AlphaRemover( new CamelNameConverter() )
    assertThat( converter.columnName( "@camelName" ), equalTo( "CAMEL_NAME" ) )
  }
}