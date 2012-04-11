/* TstString.scala
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
package no.antares.dbunit.model

/** Model class that maps to table with String-type columns */
case class TstString(
    var colWithString: String
) {
	def this() = this( null );
}

object TstString {
  val sqlSelectAll	= "SELECT * FROM TST_STRINGS"
  def sqlSelectMatching( matsj: String )	= sqlSelectAll + " WHERE COL_WITH_STRING = 'X'".replaceAll( "X", matsj )
  def sqlDropScript	= "drop table tst_strings"
	def sqlCreateScript	= """create table tst_strings (
	COL_WITH_STRING varchar(255) PRIMARY KEY
);
""";

  def flatXmlTestData	= """<?xml version='1.0' encoding='UTF-8'?>
<dataset>
  <tst_strings COL_WITH_STRING="GODKJENT" />
  <tst_strings COL_WITH_STRING="28%" />
  <tst_strings COL_WITH_STRING="<![CDATA[Middels/Høy risiko]]>" />
  <tst_strings COL_WITH_STRING="Up&#229;klagelig" />
  <tst_strings COL_WITH_STRING="13.5/19.5%" />
</dataset>"""
;

  def oldDbUnitXmlTestData	= """<?xml version='1.0' encoding='UTF-8'?>
<dataset>
  <table name="tst_strings">
    <column>COL_WITH_STRING</column>
    <row>
      <value>GODKJENT</value>
    </row>
    <row>
      <null/>
    </row>
    <row>
      <value>28%</value>
    </row>
    <row>
      <value><![CDATA[Middels/Høy risiko]]></value>
    </row>
    <row>
      <value>Up&#229;klagelig</value>
    </row>
    <row>
      <value>13.5/19.5%</value>
    </row>
  </table>
</dataset>"""
;

  val testValue1  = "1 ÆØÅ +sdlkf"
  def jsonTestData	= """{
	"dataset": {
		"tstStrings": [
			{ "colWithString": "Value1"  }
		]
	}
}""".replaceAll( "Value1", testValue1 )
;

}