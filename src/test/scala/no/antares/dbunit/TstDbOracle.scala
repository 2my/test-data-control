/* TstDbOracle.scala
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

import org.dbunit.database.DatabaseConfig

/**
 * @author tommy skodje
*/
class TstDbOracle extends DbProperties(
  "oracle.jdbc.OracleDriver",
  "FiXme",
  "FiXme", "FiXme",
  "FiXme"
) {
  dbUnitProperties.append( ( DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES, true.asInstanceOf[Object] ) );
  dbUnitProperties.append( ( DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new org.dbunit.ext.oracle.OracleDataTypeFactory() ) );
}