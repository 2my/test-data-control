package no.antares.dbunit.converters

/* CamelNameConverter.scala
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
/** A name converter that converts camelCased to UPPERCASED_UNDERSCORED
 @author Tommy Skodje
*/
class CamelNameConverter( private val nextInChain: DefaultNameConverter ) extends DefaultNameConverter() {

  def this() = this( new DefaultNameConverter() );

  override def tableName( oldName: String ): String  = nextInChain.tableName( camel2underscored( oldName ) );
  override def columnName( oldName: String ): String  = nextInChain.tableName( camel2underscored( oldName ) );

  private def camel2underscored( name: String ): String = {
    val underscored = new StringBuilder()
    for ( c <- name.toArray[Char] ) {
      if ( c.isLower || ( underscored.length == 0 ) )
        underscored.append( c.toUpper )
      else
        underscored.append( "_" ).append( c )
    }
    underscored.toString
  }

}