package no.antares.dbunit;

import no.antares.dbunit.converters.CamelNameConverter;

import no.antares.dbunit.model.TstString;
import org.codehaus.jettison.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.File;

/** Mostly for demonstration purpose
 * @author tommyskodje
 */
public class DbWrapperJavaTest {
    // DbProperties props	= new DbProperties( "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:derbyDB;create=true", "TEST", "TEST", "TEST" );
    // DbWrapper db	= new DbWrapper( props );
    static final DbWrapper db	= new TstDbDerby();
    static final String[] scripts    = { TstString.sqlDropScript(), TstString.sqlCreateScript() };

    @BeforeClass public static void setUpClass() {
        db.runSqlScripts( scripts );
    }

    @Test
    public void testJsonToDB_simple() throws Exception {
        JsonDataSet jsonSet	= new JsonDataSet( TstString.jsonTestData(), new CamelNameConverter() );
        db.refreshWithFlatJSON( jsonSet );

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
        db.refreshWithFlatJSON( jsonSet );

        JSONObject json	= new JSONObject( TstString.jsonTestData() );
        String expected  = json.getJSONObject( "dataset" ).getJSONArray("tstStrings" ).getJSONObject( 0 ) .getString( "colWithString" );

        /*  TODO: verify
        JavaConversions.asScalaBuffer( null )
        scala.Tuple2 p  = new scala.Tuple2( "tstStrings", "SELECT * FROM TST_STRINGS" );
        Object result = db.extractFlatXml( p );
        */
    }

}
