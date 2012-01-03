package no.antares.dbunit

/* JsonDataSetTest.scala
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

import org.junit.{After, Test}
import org.slf4j.{LoggerFactory, Logger}
import converters._
import java.lang.Boolean
import org.codehaus.jettison.json.{JSONObject, JSONArray}
import org.dbunit.dataset.ITableMetaData

/** @author Tommy Skodje */
class JsonDataSetTest extends AssertionsForJUnit {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[JsonDataSetTest] )

  @Test def test_tableParsing() {
    val jsonSet	= new JsonDataSet( mixedJsonData )
    val t = jsonSet.tables

    assert( 2 == t.size )

    assert( "NAMET"  === t(0).tableName )
    assert( 4 == t(0).metaData.getColumns.length )

    assert( "LIMT"   === t(1).tableName )
    assert( 2 == t(1).metaData.getColumns.length )
	}

  @Test def test_rowParsing() {
    val jsonSet	= new JsonDataSet( mixedJsonData )
    val t = jsonSet.tables

    assert( 4 == t(0).rows().size )
    assertContains( t(0).row(0), ("ID", 1 ), ("M1", "a" ) )
    assertContains( t(0).row(1), ("ID", 2 ), ("M2", "b" ) )
    assertContains( t(0).row(2), ("ID", 3 ), ("M2", "c" ) )
    assertContains( t(0).row(3), ("ID", 4 ), ("M1", "e" ) )

    assert( 3 == t(1).rows().size )
    assertContains( t(1).row(0), ("ID", 1 ), ("LIM", 4 ) )
    assertContains( t(1).row(1), ("ID", 2 ), ("LIM", 3 ) )
    assertContains( t(1).row(2), ("ID", 4 ), ("LIM", 1 ) )
	}

  private def assertContains( row: List[ ColumnInDataSet ], expected: Tuple2[ String, Any ]*) {
    expected.foreach( expect => {
      row.find( check(_) ) match {
        case c: Some[ ColumnInDataSet ] => ;
        case _ => fail( expect + " not found in columns: " + row.foldLeft( "" )( (str, col) => str + col.toString + ", " )  )
      }
      def check( p: ColumnInDataSet ): Boolean = (p.colName == expect._1 ) && (p.value == expect._2 )
    });
  }

  val mixedJsonData =
"""{
	"petter1": {
		"NAMET": { "ID": 1, "NAME": "petter", "M1": "a" },
		"LIMT": { "ID": 1, "LIM": 4 }
	},
	"petter2": {
		"NAMET": { "ID": 2, "NAME": "petter", "M2": "b" },
		"LIMT": { "ID": 2, "LIM": 3 }
	},
	"JunitTestWs": {
		"NAMET": { "ID": 3, "NAME": "JunitTestWs", "M2": "c", "M1": "d" }
	},
	"Espen": {
		"NAMET": { "ID": 4, "NAME": "ESPEN", "M1": "e", "M2": "f" },
		"LIMT": { "ID": 4, "LIM": 1 }
	}
}""";

}
