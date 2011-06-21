/* JsonDbUnitAntTask.scala
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

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

/** http://ant.apache.org/manual/develop.html.
 @author Tommy Skodje
*/
class JsonDbUnitAntTask extends Task {

  var msg: String = null;

  /** The method executing the task */
  override def execute(): Unit = {
    println( msg );
  }

  /** The setter for the "message" attribute */
  def setMessage( msgIn: String ): Unit = {
    this.msg = msgIn;
  }

}