/* TstDbOracle.scala
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

import org.slf4j.{LoggerFactory, Logger}
import java.util.Properties
import java.io.{OutputStream, InputStream}
import org.apache.derby.tools.ij
import java.sql.{DriverManager, Connection}
import org.dbunit.util.fileloader.FlatXmlDataFileLoader
import org.dbunit.database.DatabaseConnection
import org.dbunit.operation.DatabaseOperation

/** @author Tommy Skodje */
class TstDbOracle extends AbstractDB {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[TstDbDerby] )

  private var dbConnection: Connection	= null;

  private var created	= false;

	private var dbName = "derbyDB";
	private val connectionProperties = new Properties();

	connectionProperties.put("user", "TEST");
	connectionProperties.put("username", "TEST");
	connectionProperties.put("password", "TEST");

	try {
		loadDriver( "org.apache.derby.jdbc.EmbeddedDriver" );
		dbConnection	= connect( true );
  } catch {
    case ex: Throwable => {
      catchall { () => dbConnection.close() };
      dbConnection	= null;
    }
  } finally {
	}

  override def connection(): Connection	= dbConnection;

  override def connect(): Connection	= connect( false );

  override def scriptRunner( script: Encoded[ InputStream ] , output: Encoded[ OutputStream ] ): Boolean = {
    val result = ij.runScript( dbConnection, script.stream, script.encoding, output.stream, output.encoding );
    logger.debug( "ij.runScript, result code is: " + result );
    return (result==0);
  }

	private def connect( create: Boolean ):  Connection	= {
		var url	= "jdbc:derby:" + dbName;
		if ( create )
			url	+= ";create=true";
		return DriverManager.getConnection( url, connectionProperties);
	}

  private def loadDatabase( dbDataFile: String ) {
		// IDataSetProducer producer = new FlatXmlProducer( new InputSource( dbDataFile ) );
		// IDataSet dataSet = new StreamingDataSet(producer);
		val dataSet = ( new FlatXmlDataFileLoader() ).load( dbDataFile );

		val conn	= new DatabaseConnection( dbConnection );
		DatabaseOperation.REFRESH.execute( conn, dataSet );
  }

	/**
	 * Loads the appropriate JDBC driver for this environment/framework. For
	 * example, if we are in an embedded environment, we load Derby's
	 * embedded Driver, <code>org.apache.derby.jdbc.EmbeddedDriver</code>.
	 */
	private def loadDriver( driver: String ): Unit = {
		/*
		 *  The JDBC driver is loaded by loading its class.
		 *  If you are using JDBC 4.0 (Java SE 6) or newer, JDBC drivers may
		 *  be automatically loaded, making this code optional.
		 *
		 *  In an embedded environment, this will also start up the Derby
		 *  engine (though not any databases), since it is not already
		 *  running. In a client environment, the Derby engine is being run
		 *  by the network server framework.
		 *
		 *  In an embedded environment, any static Derby system properties
		 *  must be set before loading the driver to take effect.
		 */
		try {
			Class.forName(driver).newInstance();
			System.out.println("Loaded the appropriate driver");
		} catch {
      case ex: ClassNotFoundException => {
        System.err.println("\nUnable to load the JDBC driver " + driver);
        System.err.println("Please check your CLASSPATH.");
        ex.printStackTrace(System.err);
      }
      case ex: InstantiationException => {
  			System.err.println(
					"\nUnable to instantiate the JDBC driver " + driver);
	  		ex.printStackTrace(System.err);
      }
      case ex: IllegalAccessException => {
  			System.err.println(
					"\nNot allowed to access the JDBC driver " + driver);
	  		ex.printStackTrace(System.err);
      }
    }
	}

}