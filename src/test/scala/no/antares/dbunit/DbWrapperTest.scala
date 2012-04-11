/* DbWrapperTest.scala
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

import java.lang.Float
import org.scalatest.junit.AssertionsForJUnit
import org.codehaus.jettison.json.JSONObject

import collection.mutable.ListBuffer

import no.antares.dbunit.model._
import java.text.SimpleDateFormat
import xml.{Node, Elem, XML}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.slf4j.{LoggerFactory, Logger}
import java.util.{ArrayList, Date}
import no.antares.util.FileUtil
import converters._
import org.junit.{Before, After, Test}
import org.scalatest.Assertions._
import no.antares.xstream.XStreamUtils

/**
 * @author Tommy Skodje
*/
@RunWith(classOf[Parameterized])
class DbWrapperTest( val dbp: DbProperties ) extends AssertionsForJUnit {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[DbWrapperTest] )

  val db  = new DbWrapper( dbp );

  @Before def setUp(): Unit  = {
    dbp.runSqlScripts( TstString.sqlDropScript, TstString.sqlCreateScript );
    dbp.runSqlScripts( Credential.sqlDropScript, Credential.sqlCreateScript );
  }

  @After def cleanUp(): Unit  = dbp.rollback()

  @Test def refreshWithFlatJSON_simple() {
    val jsonSet	= new JsonDataSet( TstString.jsonTestData, new CamelNameConverter() )

    db.refreshWithFlatJSON( jsonSet )

    val result = ( db.extractFlatXml( ("dummy", TstString.sqlSelectAll ) )  \\ "@COL_WITH_STRING" text )
		assert( TstString.testValue1 === result )
  }

  @Test def deleteMatchingFlatJSON() {
    refreshWithFlatJSON_simple()
    val jsonSet	= new JsonDataSet( TstString.jsonTestData, new CamelNameConverter() )

    db.deleteMatchingFlatJSON( jsonSet )

    val result = ( db.extractFlatXml( ("dummy", TstString.sqlSelectAll ) )  \\ "@COL_WITH_STRING" text )
		assert( result.isEmpty )
	}

  @Test def refreshWithFlatJSON_file() {
    val jsonSet	= new JsonDataSet( FileUtil.getFromClassPath( "credentialz.json" ), new ConditionalCamelNameConverter() )

    db.refreshWithFlatJSON( jsonSet )

    val result = ( db.extractFlatXml( ("dummy", Credential.sqlSelectAll) )  \\ "@USER_NAME" text)
		assert( Credential.userNameFromFile === result )
	}

  @Test def extractFlatXml() {
    db.refreshWithFlatXml( Credential.flatXmlTestData )

    val partialResult = ( db.extractFlatXml( ("dummy", Credential.sqlSelectAll) )  \\ "@USER_NAME" text)

		assert( Credential.userNameFromXml  === partialResult )
  }

  @Test def extractFlatJson() {
    db.refreshWithFlatXml( Credential.flatXmlTestData )

    val json = db.extractFlatJson( ("credentialz", Credential.sqlSelectAll) )

		val result  = Credential.from( json.getJSONObject( "credentialz" ) );
    assert( Credential.userNameFromXml  === result.user )
  }

  // @Test  // TODO: too much data in test database
  def stream2FlatXml() {
    // if ( "oracle.jdbc.OracleDriver" == db.db.driver ) return;
    db.refreshWithFlatXml( Credential.flatXmlTestData )
    val expectedXml = XML.loadString(Credential.flatXmlTestData)

		val fullResult	= db.stream2FlatXml()

		assert( (expectedXml \\ "@USER_NAME" text)  === (fullResult \\ "@USER_NAME" text) )
		logger.info( fullResult \\ "@NAME" text )
  }

  private def parseTstString( row: Elem ): TstString = new TstString( (row \\ "@COL_WITH_STRING" ).text )

  private def parseTstNumerical( row: Elem ): TstNumerical = {
    val i = (row \\ "@COL_WITH_INT" ).text
    val f = ( row \\ "@COL_WITH_FLOAT" ).text
    val d = ( row \\ "@COL_WITH_DATE" ).text
    new TstNumerical( toInt( i ), toFloat( f ), toDate( d ) );
  }

  private def parseTstNumerical( jsonO: JSONObject ): TstNumerical = {
    val date  = new Date()	// jsonO.getString( "colWithDate" )
    val fpnum	= jsonO.getDouble( "colWithFloat" ).asInstanceOf[Float]
    new TstNumerical( jsonO.getInt( "colWithInt" ), fpnum, date );
  }

  private def xmlElements( nodes: Node, elementName: String ): List[Elem] = {
    val buf = new ListBuffer[Elem]
    ( nodes \\ elementName ).foreach( node => buf.append( node.asInstanceOf[Elem] ) )
    buf.toList
  }

  private val dateparser  = new SimpleDateFormat("yyyy-MM-dd")
  private def toDate( value: String ): Date = if ( value.isEmpty ) null else dateparser.parse("2008-05-06 13:29")
  private def toInt( value: String ): java.lang.Integer = if ( value.isEmpty ) null else value.toInt
  private def toFloat( value: String ): java.lang.Float = if ( value.isEmpty ) null else value.toFloat

}

// TODO: use FunSuite http://www.scalatest.org/scaladoc-1.6.1/#org.scalatest.FunSuite
// see: http://jpz-log.info/archives/2009/09/29/scalatest-in-maven/
object DbWrapperTest {
  @Parameters
  def configurationsPresent(): ArrayList[ Array[Object] ] = {
    val configurations	= new ArrayList[ Array[Object] ]

    // Test with in-memory DB, default.
    configurations.add( Array( new DbDerby() ) )
    // configurations.add( Array( new TstDbOracle() ) )

    return configurations
  }
}
