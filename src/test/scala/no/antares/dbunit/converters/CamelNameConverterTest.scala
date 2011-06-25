package no.antares.dbunit.converters

/* CamelNameConverterTest.scala
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
import org.scalatest.Assertions._

import no.antares.dbunit.converters.CamelNameConverter

/** @author tommyskodje */

class CamelNameConverterTest  extends AssertionsForJUnit {

  @Test def testDataSetName() = {
    var converter  = new CamelNameConverter()
    assert( "dataset"  === converter.dataSetName() )
    converter  = new CamelNameConverter( "list" )
    assert( "list"  === converter.dataSetName() )
  }

  @Test def testTableName() = {
    var converter  = new CamelNameConverter()
    assert( "CAMEL_NAME"  === converter.tableName( "camelName" ) )
    assert( "CAMEL"  === converter.tableName( "camel" ) )
    assert( "SMOKING_KILL_YOU"  === converter.tableName( "SmokingKillYou" ) )
  }

  @Test def testColumnName() = {
    var converter  = new CamelNameConverter()
    assert( "CAMEL_NAME"  === converter.columnName( "camelName" ) )
    assert( "CAMEL"  === converter.columnName( "camel" ) )
    assert( "SMOKING_KILL_YOU"  === converter.columnName( "SmokingKillYou" ) )
  }
}