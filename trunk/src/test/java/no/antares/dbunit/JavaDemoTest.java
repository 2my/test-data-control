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

import no.antares.dbunit.converters.AlphaRemover;
import no.antares.dbunit.converters.CamelNameConverter;
import no.antares.dbunit.converters.ConditionalCamelNameConverter;
import no.antares.dbunit.model.TstString;
import no.antares.xstream.XStreamUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

/** Mostly for demonstration purpose
 * @author tommyskodje
 */
public class JavaDemoTest {
	private final static DbDataSource dbDataSource	= new DbDataSource(
			"org.apache.derby.jdbc.EmbeddedDriver",
			"jdbc:derby:derbyDB;create=true",
			"TEST",
			"TEST",
			"TEST"
		);

    /** Full roundtrip: run db scripts, export, refresh and delete again */
    @Test public void testDbWrapper() throws Exception {
        Db db	= new DbDerby();
        // Db db	= dbDataSource;
        DbWrapper wrapper	= new DbWrapper( db );

        // clean db
        String[] scripts    = { TstString.sqlDropScript(), TstString.sqlCreateScript() };
        db.runSqlScripts( scripts );
        // export and verify empty
        JSONObject emptyResult = new JSONObject( wrapper.extractFlatJson( "tst_strings", TstString.sqlSelectAll() ) );
        System.out.println( emptyResult.toString(2) );
        assertNull( emptyResult.optJSONObject( "tst_strings" ) );

        // refresh
        String val1 = "1 ÆØÅ +sdlkf";
        JsonDataSet jsonSet	= new JsonDataSet( TstString.jsonTestData( val1 ), new CamelNameConverter() );
        wrapper.refreshWithFlatJSON( jsonSet );
        // export
        JSONObject resultWithAlpha = new JSONObject( wrapper.extractFlatJson( "tst_strings", TstString.sqlSelectAll() ) );
        // System.out.println(resultWithAlpha.toString(2));
        assertEquals( val1, resultWithAlpha.getJSONObject("tst_strings" ).getString( "@COL_WITH_STRING" ) );

        // delete
        ColumnFilter filter   = new ColumnFilter();
        filter.addTableJ( "TST_STRINGS", new String[] {"COL_WITH_STRING"} );
        wrapper.addUnitProperty( "http://www.dbunit.org/properties/primaryKeyFilter", filter );
        wrapper.deleteMatchingFlatJSON(jsonSet);
        // export
        emptyResult = new JSONObject( wrapper.extractFlatJson( "tst_strings", TstString.sqlSelectAll() ) );
        System.out.println( emptyResult.toString(2) );
        assertNull( emptyResult.optJSONObject( "tst_strings" ) );

        // refresh with wrapping and chained converters (does nothing useful here, because data underscored)
        jsonSet	= new JsonDataSet( resultWithAlpha.toString(), new AlphaRemover( new ConditionalCamelNameConverter() ) );
        wrapper.refreshWithFlatJSON( jsonSet.wrap() );
        resultWithAlpha = new JSONObject( wrapper.extractFlatJson( "tst_strings", TstString.sqlSelectAll() ) );
        assertEquals( val1, resultWithAlpha.getJSONObject("tst_strings" ).getString( "@COL_WITH_STRING" ) );
    }

    /** XStream to and fro json */
    @Test public void testXStreamUtils() throws Exception {
        XStreamUtils xStream    = new XStreamUtils();
        TstObj tstObj   = new TstObj();
        tstObj.field    = "1 æøå";

        // simple back and forth
        String json = XStreamUtils.toJson( tstObj );
        assertTrue( 0 < json.indexOf( tstObj.field ) );
        TstObj reverse   = (TstObj) xStream.fromJson( json );
        assertEquals( tstObj.field, reverse.field );

        // now with aliases
        json = "{\"obj\": { \"f\": \"æøå\"  }}";
        xStream.alias( "obj", TstObj.class );
        xStream.aliasField( TstObj.class, "field", "f" );
        TstObj result    = (TstObj) xStream.fromJson( json );
        assertEquals( "æøå", result.field );
    }
    private class TstObj {
        private String field    = null;
    }

}
