/* TstDbDerby.scala
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.io._

import org.apache.derby.tools.ij;

import org.codehaus.jettison.json.JSONObject

import org.dbunit.operation.DatabaseOperation;
import org.dbunit.util.fileloader.FlatXmlDataFileLoader
import org.dbunit.dataset.xml.{FlatXmlDataSet, FlatXmlDataSetBuilder}
import org.dbunit.database._
import org.dbunit.dataset.{CachedDataSet, IDataSet}

import org.xml.sax.InputSource
;

/** Code for in-memory database
 * 
 * @author Tommy Skodje
 */
class TstDbDerby {
	private var created	= false;

	private var dbName = "derbyDB";
	private var connection: Connection	= null;
	private val connectionProperties = new Properties();

	connectionProperties.put("user", "TEST");
	connectionProperties.put("username", "TEST");
	connectionProperties.put("password", "TEST");

	try {
		loadDriver( "org.apache.derby.jdbc.EmbeddedDriver" );
		connection	= connect( true );
  } catch {
    case ex: Throwable => {
      catchall { () => connection.close() };
      connection	= null;
    }
  } finally {
	}

	def connect():  Connection	= connect( false );

	private def connect( create: Boolean ):  Connection	= {
		var url	= "jdbc:derby:" + dbName;
		if ( create )
			url	+= ";create=true";
		return DriverManager.getConnection( url, connectionProperties);
	}

  def refreshWithFlatJSON( jsonS: String ): Unit = {
    val json	= new JSONObject( jsonS )
    val dataSet = new CachedDataSet( new FlatJsonDataSetProducer( json ) )
    val dbuConnection = new DatabaseConnection(connection);
    DatabaseOperation.REFRESH.execute( dbuConnection, dataSet );
  }

  def refreshWithFlatXml( xml: String ): Unit = {
    val builder = new FlatXmlDataSetBuilder();
    val is = new StringReader( xml );
    val dataSet = builder.build(new InputSource(is));

    val dbuConnection = new DatabaseConnection(connection);
    DatabaseOperation.REFRESH.execute(dbuConnection, dataSet);
  }

  /** partial database export */
  def extractFlatXml( tablesWithQueries: (String,String)* ): String = {
    val dbuConnection = new DatabaseConnection( connection );
    val partialDataSet = new QueryDataSet(dbuConnection);
    for ( (name, select) <- tablesWithQueries )
      partialDataSet.addTable( name, select );
    val partialResultW: StringWriter = new StringWriter();
    FlatXmlDataSet.write(partialDataSet, partialResultW);
    partialResultW.toString
  }

  /** full database export - setup for streaming @see http://www.dbunit.org/faq.html#streaming */
  def stream2FlatXml(): String = {
    val dbuConnection = new DatabaseConnection( connection );
		val config = dbuConnection.getConfig();
		config.setProperty( DatabaseConfig.PROPERTY_RESULTSET_TABLE_FACTORY, new ForwardOnlyResultSetTableFactory() );
		val fullDataSet = dbuConnection.createDataSet();
		val fullResultW	= new StringWriter();
		FlatXmlDataSet.write(fullDataSet, fullResultW );
		fullResultW.toString
  }

  private def loadDatabase( dbDataFile: String ) {
		// IDataSetProducer producer = new FlatXmlProducer( new InputSource( dbDataFile ) );
		// IDataSet dataSet = new StreamingDataSet(producer);
		val dataSet = ( new FlatXmlDataFileLoader() ).load( dbDataFile );

		val conn	= new DatabaseConnection( connection );
		DatabaseOperation.REFRESH.execute( conn, dataSet );
  }

	def runSqlScriptFile( scriptFile: File ): Boolean = {
		try {
			return runSqlScript( new FileInputStream(scriptFile) );
		} catch {
		  case ex: FileNotFoundException => return false;
		}
	}

	def runSqlScript( script: String ): Boolean = {
		return runSqlScript( new ByteArrayInputStream( script.getBytes() ) );
	}

	def runSqlScript( script: InputStream ): Boolean = {
		try {
			connection.setAutoCommit(false);
			val result = ij.runScript( connection, script,"UTF-8",System.out,"UTF-8");
			println("Result code is: " + result);
			connection.commit();
			return (result==0);
		} finally {
			catchall { () => script.close() };
		}
	}

	def catchall( f : () => Unit ): Unit = {
		try {
	   	f()
    } catch {
      case t: Throwable => ;	// ignore
    }
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
