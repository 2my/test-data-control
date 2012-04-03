/* ScriptRunner.scala
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
package no.antares.dbunit

import org.slf4j.{LoggerFactory, Logger}
import java.io._
import org.apache.commons.io.IOUtils
import org.apache.tools.ant.taskdefs.SQLExec
import org.apache.tools.ant.Project

/**
 * @author tommy skodje
*/
class ScriptRunner(
  val driver: String,
  val dbUrl: String,
  val username: String,
  val password: String
) {

  private final val logger: Logger = LoggerFactory.getLogger( classOf[ScriptRunner] )

  /** Use ant (http://stackoverflow.com/questions/2071682/how-to-execute-sql-script-file-in-java) */
  def executeSql( script: String ): Boolean = {
    try {
      val executer = new SQLExec()
      val project = new Project();
      project.init();
      executer.setProject(project);
      executer.setTaskType("sql");
      executer.setTaskName("sql");

      // executer.setSrc(new File(sqlFilePath));
      executer.setDriver( driver );
      executer.setPassword( password );
      executer.setUserid( username );
      executer.setUrl( dbUrl );

      executer.addText( script )
      executer.execute();
    } catch {
      case t: Throwable => return false;
    }
    true;
  }

  /**  */
  def runSqlScriptFile( scriptFile: File ): Boolean = {
    logger.debug( "runSqlScriptFile: " + scriptFile );
    try {
      val writer = new StringWriter();
      IOUtils.copy( new FileInputStream(scriptFile), writer, "UTF-8" )
      return executeSql( writer.toString() )
    } catch {
      case ex: FileNotFoundException => return false;
    }
  }

}