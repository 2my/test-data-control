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
import com.sun.tools.javac.comp.Check
import java.lang.Boolean
import org.codehaus.jettison.json.{JSONObject, JSONArray}

/** @author Tommy Skodje */
class JsonDataSetTest extends AssertionsForJUnit {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[JsonDataSetTest] )

  @Test def testJsonToDB_simple() {
    val jsonSet	= new JsonDataSet( mixedJsonData )
    val t = jsonSet.tables()

    assert( "NAMET"  === t(0).tableName )
    assertContains( t(0).row(0), ("ID", 1 ) )

    assert( "LIMT"  === t(1).tableName )
    assertContains( t(1).row(0), ("ID", 1 ), ("LIM", 4 ) )

    assert( "LIMT"  === t(3).tableName )
    assertContains( t(3).row(0), ("ID", 2 ), ("LIM", 3 ) )

    assert( "NAMET"  === t(5).tableName )
    assertContains( t(5).row(0), ("ID", 4 ) )

    assert( "LIMT"  === t(6).tableName )
    assertContains( t(6).row(0), ("ID", 4 ), ("LIM", 1 ) )
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
	"petterIkea": {
		"NAMET": { "ID": 1, "NAME": "petter" },
		"LIMT": { "ID": 1, "LIM": 4 }
	},
	"petterDemok": {
		"NAMET": { "ID": 2, "NAME": "petter" },
		"LIMT": { "ID": 2, "LIM": 3 }
	},
	"JunitTestWs": {
		"NAMET": { "ID": 3, "NAME": "JunitTestWs" }
	},
	"Espen": {
		"NAMET": { "ID": 4, "NAME": "ESPEN" },
		"LIMT": { "ID": 4, "LIM": 1 }
	}
}""";

}
