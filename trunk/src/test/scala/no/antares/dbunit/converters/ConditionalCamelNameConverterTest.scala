package no.antares.dbunit.converters

/* ConditionalCamelNameConverterTest.scala
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
import org.scalatest.junit.AssertionsForJUnit
import org.junit.Test

import no.antares.dbunit.converters.ConditionalCamelNameConverter


/** @author Tommy Skodje */
class ConditionalCamelNameConverterTest extends AssertionsForJUnit {

  @Test def testDataSetName() = {
    var converter  = new CamelNameConverter()
    assert( "dataset"  === converter.dataSetName() )
    converter  = new CamelNameConverter( "list" )
    assert( "list"  === converter.dataSetName() )
  }

  @Test def testTableName() = {
    var converter  = new ConditionalCamelNameConverter()
    assert( "CAMEL_NAME"  === converter.tableName( "camelName" ) )
    assert( "camel"  === converter.tableName( "camel" ) )
    assert( "SMOKING_KILL_YOU"  === converter.tableName( "SmokingKillYou" ) )

    assert( "CAMEL_NAME"  === converter.tableName( "CAMEL_NAME" ) )
  }

  @Test def testColumnName() = {
    var converter  = new ConditionalCamelNameConverter()
    assert( "CAMEL_NAME"  === converter.columnName( "camelName" ) )
    assert( "camel"  === converter.columnName( "camel" ) )
    assert( "SMOKING_KILL_YOU"  === converter.columnName( "SmokingKillYou" ) )

    assert( "CAMEL_NAME"  === converter.columnName( "CAMEL_NAME" ) )
  }
}