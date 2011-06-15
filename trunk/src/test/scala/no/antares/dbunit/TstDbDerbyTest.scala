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


import java.lang.Float
import java.lang.Integer
import java.util.Date


import org.scalatest.junit.AssertionsForJUnit
import org.codehaus.jettison.json.JSONObject

import scala.collection.JavaConversions._
import collection.mutable.ListBuffer

import no.antares.dbunit.model._
import no.antares.xstream.XStreamUtils
import java.text.SimpleDateFormat
import xml.{Node, Elem, XML}
import org.junit.{After, Before, Test}

class TstDbDerbyTest extends AssertionsForJUnit {

  val db	= new TstDbDerby();

  @After def cleanUp  = db.rollback()

  @Test def verify_extractFlatXml() {
    db.runSqlScript( Credential.sqlCreateScript );
    db.refreshWithFlatXml( Credential.flatXmlTestData )
    val expectedXml = XML.loadString(Credential.flatXmlTestData)

    val partialResult = db.extractFlatXml( ("credential", "SELECT * FROM credential") )

		assert( (expectedXml \\ "@USER_NAME" text)  === (partialResult \\ "@USER_NAME" text) )
		println( partialResult \\ "@PASS_WORD" text )
  }

  @Test def verify_stream2FlatXml() {
    db.runSqlScript( Credential.sqlCreateScript );
    db.refreshWithFlatXml( Credential.flatXmlTestData )
    val expectedXml = XML.loadString(Credential.flatXmlTestData)

		val fullResult	= db.stream2FlatXml()

		assert( (expectedXml \\ "@USER_NAME" text)  === (fullResult \\ "@USER_NAME" text) )
		println( fullResult \\ "@NAME" text )
  }

  @Test def testJsonToDB_simple() {
    db.runSqlScript( TstString.sqlCreateScript );
    db.refreshWithFlatJSON( TstString.jsonTestData )

    val json	= new JSONObject( TstString.jsonTestData )
    val expected  = json.getJSONObject( "dataset" ).getJSONArray("tstStrings" ).getJSONObject( 0 ) .getString( "colWithString" );
    println( expected )

    val result = db.extractFlatXml( ("tstStrings", "SELECT * FROM TST_STRINGS") )
		assert( expected  === (result \\ "@COL_WITH_STRING" text) )
	}

  @Test def testJsonToDB_variants() {
    db.runSqlScript( TstNumerical.sqlCreateScript );

    db.refreshWithFlatJSON( TstNumerical.jsonTestData )

    val resultX = db.extractFlatXml( ("tstNumericals", "SELECT * FROM TST_NUMERICALS") )
    val resultL  = xmlElements( resultX, "tstNumericals" ).map( elem => parseTstNumerical( elem ) )
    assert( resultL(0) == (new TstNumerical( 456, null, null )) )
    assert( resultL(1) == (new TstNumerical( 789, 3.141592658f, null )) )
    assert( resultL(2) == (new TstNumerical( -3, null, toDate( "2004-09-30" ) )) )
    assert( resultL(3) == (new TstNumerical( 123, -2f, toDate( "2114-12-24" ) )) )
	}

  def xmlElements( nodes: Node, elementName: String ): List[Elem] = {
    val buf = new ListBuffer[Elem]
    ( nodes \\ elementName ).foreach( node => buf.append( node.asInstanceOf[Elem] ) )
    buf.toList
  }

  def parseTstString( row: Elem ): TstString = new TstString( (row \\ "@COL_WITH_STRING" ).text )

  def parseTstNumerical( row: Elem ): TstNumerical = {
    val i = (row \\ "@COL_WITH_INT" ).text
    val f = ( row \\ "@COL_WITH_FLOAT" ).text
    val d = ( row \\ "@COL_WITH_DATE" ).text
    new TstNumerical( toInt( i ), toFloat( f ), toDate( d ) );
  }

  def parseTstNumerical( jsonO: JSONObject ): TstNumerical = {
    val date  = new Date()	// jsonO.getString( "colWithDate" )
    val fpnum	= jsonO.getDouble( "colWithFloat" ).asInstanceOf[Float]
    new TstNumerical( jsonO.getInt( "colWithInt" ), fpnum, date );
  }

  val dateparser  = new SimpleDateFormat("yyyy-MM-dd")
  def toDate( value: String ): Date = if ( value.isEmpty ) null else dateparser.parse("2008-05-06 13:29")
  def toInt( value: String ): java.lang.Integer = if ( value.isEmpty ) null else value.toInt
  def toFloat( value: String ): java.lang.Float = if ( value.isEmpty ) null else value.toFloat

}