/* TstNumerical.scala
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


/** Model class that maps to table with some numerical type columns */
case class TstNumerical(
    var colWithInt: java.lang.Integer,
    var colWithFloat: java.lang.Float,
    var colWithDate: java.util.Date
) {
  def this()	= this( null, null, null );	// auxillary constructor
}

object TstNumerical {
  def sqlDropScript	= "drop table tst_numericals"
	def sqlCreateScript	= """create table tst_numericals (
    COL_WITH_INT integer PRIMARY KEY,
    COL_WITH_FLOAT float,
    COL_WITH_DATE date
  );
  """;

	def oldDbUnitXmlTestData	= """<?xml version='1.0' encoding='UTF-8'?>
  <dataset>
    <table name="tst_numericals">
      <column>COL_WITH_INT</column>
      <column>COL_WITH_FLOAT</column>
      <column>COL_WITH_DATE</column>
      <row>
        <value>456</value>
        <value>0.959106</value>
        <value>2004-09-30</value>
      </row>
      <row>
        <value>-1</value>
        <value>3.14</value>
        <value>2005</value>
      </row>
      <!-- row>
        <null/>
        <null/>
        <null/>
      </row> -->
    </table>
  </dataset>"""
  ;


  // below I tried to jog the json producer by switching columns, order etc.
  def jsonTestData	= """{
	"dataset": {
		"tstNumericals": [
        { "colWithInt": 456 },
        { "colWithFloat": 3.141592658,
          "colWithInt": 789
        },
        { "colWithDate": "2004-09-30",
          "colWithInt": -3
        },
        { "colWithInt": 123,
          "colWithFloat": -2,
          "colWithDate": "2114-12-24"
        }
      ]
    }
  }"""
  ;
/* TODO:
 "colWithFloat": null,
  "colWithDate": "2005",
  "colWithInt": 33
*/
}