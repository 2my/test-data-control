/* TstDbDerbyTest.scala
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
package no.antares.dbunit;

import no.antares.dbunit.model._

import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

import org.codehaus.jettison.json.JSONObject

import scala.xml.XML
import scala.collection.JavaConversions._
import no.antares.xstream.XStreamUtils

class TstDbDerbyTest extends AssertionsForJUnit {

  @Before def initialize() {
    println( "HELLO WORLD" )
  }

  @Test def verifyXml2DB2Xml() {
    val db	= new TstDbDerby();
    db.runSqlScript( Credential.sqlCreateScript );
    db.refreshWithFlatXml( Credential.flatXmlTestData )
    val expectedXml = XML.loadString(Credential.flatXmlTestData)

    val partialResult = db.extractFlatXml( ("credential", "SELECT * FROM credential") )
		val partialXml	= XML.loadString( partialResult )
		assert( (expectedXml \\ "@USER_NAME" text)  === (partialXml \\ "@USER_NAME" text) )
		println( partialXml \\ "@PASS_WORD" text )

		// full database export - setup for streaming @see http://www.dbunit.org/faq.html#streaming
		val fullResult	= db.stream2FlatXml()
		val fullXml	= XML.loadString( fullResult )
		assert( (expectedXml \\ "@USER_NAME" text)  === (fullXml \\ "@USER_NAME" text) )
		println( partialXml \\ "@NAME" text )
  }

  @Test def testJsonToDB_simple() {
    val db	= new TstDbDerby();
    db.runSqlScript( TstString.sqlCreateScript );
    db.refreshWithFlatJSON( TstString.jsonTestData )

    val json	= new JSONObject( TstString.jsonTestData )
    val expected  = json.getJSONObject( "dataset" ).getJSONArray("tstStrings" ).getJSONObject( 0 ) .getString( "colWithString" );
    println( expected )

    val result = db.extractFlatXml( ("tstStrings", "SELECT * FROM TST_STRINGS") )
		val xml	= XML.loadString( result )
		assert( expected  === (xml \\ "@COL_WITH_STRING" text) )
	}

  @Test def testJsonToDB_variants() {
    val db	= new TstDbDerby();
    db.runSqlScript( TstNumerical.sqlCreateScript );
    db.refreshWithFlatJSON( TstNumerical.jsonTestData )

    /* May need to use members, not properties: Could not access no.antares.dbunit.model.TstNumerical$.colWithInt field: colWithInt
    val util  = new XStreamUtils();
    util.alias( "dataset", List.getClass ).alias( "tstNumericals", TstNumerical.getClass )
    util.aliasField( TstNumerical.getClass, "colWithInt", "colWithInt" )
    util.aliasField( TstNumerical.getClass, "colWithFloat", "colWithFloat" )
    val expected  = util.fromJson( TstNumerical.jsonTestData )
    println( expected )
    */

    val result = db.extractFlatXml( ("tstNumericals", "SELECT * FROM TST_NUMERICALS") )
    println( result )
    val xml	= XML.loadString( result )
    // TODO assert( expected  === (xml \\ "@COL_WITH_STRING" text) )
	}

}