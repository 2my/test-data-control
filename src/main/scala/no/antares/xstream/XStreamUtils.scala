/* XStreamUtils.scala
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
package no.antares.xstream

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.xml.DomDriver
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver
import collection.mutable.ListBuffer
import com.thoughtworks.xstream.io.HierarchicalStreamDriver

/** Simple wrapper for commonly used XStream functions */
class XStreamUtils(
  val classAliases: ListBuffer[ ClassAlias ],
  val fieldAliases: ListBuffer[ FieldAlias ]
) {

  def this() = this( new ListBuffer[ ClassAlias ], new ListBuffer[ FieldAlias ] )

  def fromJson( s: String ) : Object = toObject( s, new JettisonMappedXmlDriver() );

  def fromXml( s: String ) : Object = toObject( s, new DomDriver() );

  def alias( name: String, clazz: Class[_] ): XStreamUtils = {
    classAliases.append( new ClassAlias( name, clazz ) )
    this
  }

  def aliasField( clazz: Class[_], attributeName: String, alias: String ): XStreamUtils = {
    fieldAliases.append( new FieldAlias( clazz, attributeName, alias ) )
    this
  }

  private def toObject( s: String, hierarchicalStreamDriver: HierarchicalStreamDriver ) : Object = {
    val xstream = new XStream( hierarchicalStreamDriver );
    classAliases.foreach( alias => xstream.alias( alias.xmlName, alias.clazz ) )
    fieldAliases.foreach( alias => xstream.aliasAttribute( alias.clazz, alias.attributeName, alias.alias ) )
    return xstream.fromXML( s );
  }

}

class ClassAlias( val xmlName: String, val clazz: Class[_] ) {};
class FieldAlias( val clazz: Class[_], val attributeName: String, val alias: String ) {};

object XStreamUtils {
  def toJson( o: Object ): String = {
    val xstream = new XStream( new JettisonMappedXmlDriver() );
    return xstream.toXML( o );
  }

  def toXml( o: Object ): String = {
    var xstream = new XStream( new DomDriver() );
    return xstream.toXML( o );
  }

  def fromJson( s: String ): Object = ( new XStreamUtils() ).fromJson( s );
  def fromXml( s: String ): Object = ( new XStreamUtils() ).fromXml( s );

}