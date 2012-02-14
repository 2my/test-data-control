package no.antares.dbunit;

import no.antares.dbunit.converters.CamelNameConverter;

import no.antares.dbunit.model.TstString;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;


import java.io.File;

/**
 * @author tommyskodje
 */
public class DbWrapperJavaTest {
    @Test
    public void testJsonToDB_simple() throws Exception {
        DbProperties props	= new DbProperties( "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:derbyDB;create=true", "TEST", "TEST", "TEST" );
        // DbWrapper db	= new DbWrapper( props );

        DbWrapper db	= new TstDbDerby();
        String[] scripts    = { TstString.sqlDropScript(), TstString.sqlCreateScript() };
        db.runSqlScripts( scripts );

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
