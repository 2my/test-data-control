package no.antares.dbunit

import java.sql.Connection
import org.dbunit.database.DatabaseConnection

/** @author tommyskodje */
abstract class Db {

  def connection(): Connection;

  def getDbUnitConnection(): DatabaseConnection;

  def executeSql( script: String ): Boolean;

  def rollback(): Unit;

}