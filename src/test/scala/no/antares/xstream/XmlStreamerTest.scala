/* XmlStreamerTest.scala
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
package no.antares.xstream

import java.io._
import javax.xml.stream.XMLOutputFactory

import org.codehaus.jettison.mapped.MappedXMLStreamReader
import org.codehaus.jettison.json.JSONObject
import org.scalatest.junit.AssertionsForJUnit
import org.junit.{Test, Before}
import xml.XML

import no.antares.dbunit.model.TstString
import no.antares.xstream.XmlStreamer


/**  */
class XmlStreamerTest extends AssertionsForJUnit {

  @Test def testJson2Xml() {
    println( TstString.jsonTestData );
    val tstJson	= new JSONObject( TstString.jsonTestData )

    val reader = new MappedXMLStreamReader( tstJson )
    val resultW	= new StringWriter();
    val writer = XMLOutputFactory.newInstance().createXMLStreamWriter( resultW );
    XmlStreamer.writeAll( reader, writer )

    println( resultW.toString )
    val resultXml	= XML.loadString( resultW.toString )
    assert( "1DEMOPARTNERK"  == (resultXml \\ "colWithString" text) )
	}

}
