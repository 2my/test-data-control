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
import org.junit.{After, Test}
import org.junit.runners.Parameterized
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.slf4j.{LoggerFactory, Logger}
import org.apache.derby.tools.ij
import no.antares.utils.TstContext
import java.util.{ArrayList, Date}
import java.io.{File, OutputStream, InputStream}
import no.antares.util.FileUtil
import converters._

/** @author Tommy Skodje */
@RunWith(classOf[Parameterized])
class DbWrapperTest( val db: DbWrapper ) extends AssertionsForJUnit {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[DbWrapperTest] )

  @After def cleanUp  = db.db.rollback()

  @Test def testJsonToDB_simple() {
    db.runSqlScripts( TstString.sqlDropScript, TstString.sqlCreateScript );
    val jsonSet	= new JsonDataSet( TstString.jsonTestData, new CamelNameConverter() )
    db.refreshWithFlatJSON( jsonSet )

    val json	= new JSONObject( TstString.jsonTestData )
    val expected  = json.getJSONObject( "dataset" ).getJSONArray("tstStrings" ).getJSONObject( 0 ) .getString( "colWithString" );
    logger.info( expected )

    val result = db.extractFlatXml( ("tstStrings", "SELECT * FROM TST_STRINGS") )
		assert( expected  === (result \\ "@COL_WITH_STRING" text) )
	}

  @Test def testJsonToDB_file() {
    db.runSqlScripts( Credential.sqlDropScript, Credential.sqlCreateScript );
    val jsonSet	= new JsonDataSet( FileUtil.getFromClassPath( "credentialz.json" ), new ConditionalCamelNameConverter() )
    db.refreshWithFlatJSON( jsonSet )

    val expectedXml = XML.loadString(Credential.flatXmlTestData)

    val partialResult = db.extractFlatXml( ("credentialz", "SELECT * FROM credentialz") )

		assert( (expectedXml \\ "@USER_NAME" text)  === (partialResult \\ "@USER_NAME" text) )
		logger.info( partialResult \\ "@PASS_WORD" text )
	}

  @Test def verify_extractFlatXml() {
    db.runSqlScripts( Credential.sqlDropScript, Credential.sqlCreateScript );
    db.refreshWithFlatXml( Credential.flatXmlTestData )
    val expectedXml = XML.loadString(Credential.flatXmlTestData)

    val partialResult = db.extractFlatXml( ("credentialz", "SELECT * FROM credentialz") )

		assert( (expectedXml \\ "@USER_NAME" text)  === (partialResult \\ "@USER_NAME" text) )
		logger.info( partialResult \\ "@PASS_WORD" text )
  }

  @Test def verify_extractFlatJson() {
    db.runSqlScripts( Credential.sqlDropScript, Credential.sqlCreateScript );
    db.refreshWithFlatXml( Credential.flatXmlTestData )
    val expectedXml = XML.loadString(Credential.flatXmlTestData)

    val partialResult = db.extractFlatJson( ("credentialz", "SELECT * FROM credentialz") )

		logger.info( partialResult.toString )
  }

  @Test def verify_stream2FlatXml() {
    // if ( "oracle.jdbc.OracleDriver" == db.db.driver ) return; // TODO: too much data in test database
    db.runSqlScripts( Credential.sqlDropScript, Credential.sqlCreateScript );
    db.refreshWithFlatXml( Credential.flatXmlTestData )
    val expectedXml = XML.loadString(Credential.flatXmlTestData)

		val fullResult	= db.stream2FlatXml()

		assert( (expectedXml \\ "@USER_NAME" text)  === (fullResult \\ "@USER_NAME" text) )
		logger.info( fullResult \\ "@NAME" text )
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

// TODO: use FunSuite http://www.scalatest.org/scaladoc-1.6.1/#org.scalatest.FunSuite
// see: http://jpz-log.info/archives/2009/09/29/scalatest-in-maven/
object DbWrapperTest {
  @Parameters
  def configurationsPresent(): ArrayList[ Array[Object] ] = {
    val configurations	= new ArrayList[ Array[Object] ]

    // Test with in-memory DB, default.
    configurations.add( Array( new TstDbDerby() ) )

    // should not publish our database connection - get it from xml property file
    val context  = new TstContext( "test-context-local.xml" )
    context.dataSourceOracle() match {
      case Some( properties ) => configurations.add( Array( new TstDbOracle( properties ) ) );
      case _ => ;
    }

    return configurations
  }
}

private class TstDbOracle( properties: DbProperties ) extends DbWrapper( properties ) {}

class TstDbDerby extends DbWrapper( new DbProperties( "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:derbyDB;create=true", "TEST", "TEST", "TEST" ) ) {
  private final val logger: Logger = LoggerFactory.getLogger( classOf[TstDbDerby] )
  override def runSqlScript( script: Encoded[ InputStream ] , output: Encoded[ OutputStream ] ): Boolean = {
    try {
      val result  = ij.runScript( db.connection(), script.stream, script.encoding, output.stream, output.encoding );
      logger.debug( "ij.runScript, result code is: " + result );
      return (result==0);
    } catch {
      case t: Throwable => return false;
    }
  }

}
