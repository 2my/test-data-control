/* DbWrapperJavaTest.java
   Copyright 2012 Tommy Skodje (http://www.antares.no)

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

import static org.junit.Assert.*;

import no.antares.dbunit.converters.CamelNameConverter;
import no.antares.dbunit.model.TstString;

import org.codehaus.jettison.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

/** Mostly for demonstration purpose
 * @author tommyskodje
 */
public class DbWrapperJavaTest {
    // DbProperties props	= new DbProperties( "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:derbyDB;create=true", "TEST", "TEST", "TEST" );
    // DbWrapper db	= new DbWrapper( props );
    static final Db db	= new DbDerby();
    static final DbWrapper wrapper	= new DbWrapper( db );
    // static final ScriptRunner scripter	= new ScriptRunner( db );

    static final String[] scripts    = { TstString.sqlDropScript(), TstString.sqlCreateScript() };

    @BeforeClass public static void setUpClass() {
        db.runSqlScripts( scripts );
    }

    @Test
    public void testJsonToDB_simple() throws Exception {
        JsonDataSet jsonSet	= new JsonDataSet( TstString.jsonTestData(), new CamelNameConverter() );
        wrapper.refreshWithFlatJSON( jsonSet );

        JSONObject json	= new JSONObject( TstString.jsonTestData() );
        String expected  = json.getJSONObject( "dataset" ).getJSONArray("tstStrings" ).getJSONObject( 0 ) .getString( "colWithString" );

        /*  TODO: verify
        JavaConversions.asScalaBuffer( null )
        scala.Tuple2 p  = new scala.Tuple2( "tstStrings", "SELECT * FROM TST_STRINGS" );
        Object result = db.extractFlatXml( p );
        */
    }

    @Test
    public void testJsonFromDB_simple() throws Exception {
        JsonDataSet jsonSet	= new JsonDataSet( TstString.jsonTestData(), new CamelNameConverter() );
        wrapper.refreshWithFlatJSON( jsonSet );

        JSONObject json	= new JSONObject( TstString.jsonTestData() );
        String expected  = json.getJSONObject( "dataset" ).getJSONArray("tstStrings" ).getJSONObject( 0 ) .getString( "colWithString" );

        JSONObject partialResult = wrapper.extractFlatJson( "tst_strings", "SELECT * FROM tst_strings" );
        System.out.println( partialResult.toString( 2 ) );

        String actual  = partialResult.getJSONObject("tst_strings" ).getString( "@COL_WITH_STRING" );
        assertEquals(expected, actual);
    }

}
