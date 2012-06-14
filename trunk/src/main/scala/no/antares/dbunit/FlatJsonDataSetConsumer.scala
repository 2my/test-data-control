package no.antares.dbunit

import org.dbunit.dataset.stream.IDataSetConsumer
import org.dbunit.dataset.ITableMetaData
import org.codehaus.jettison.json.JSONObject
import org.codehaus.jettison.AbstractXMLStreamWriter
import org.codehaus.jettison.mapped.{MappedNamespaceConvention, MappedXMLStreamWriter}
import java.io.StringWriter

/** Produces a dbUnit dataset from json, mirrors dbUnit FlatXmlProducer.

I chose to reimplement most of FlatXmlProducer here
I could have extended only produce() method in FlatXmlProducer, and use jettison XmlStreamReader
org.springframework.util.xml.StaxStreamXMLReader converts from XmlStreamReader to XmlReader

 @author tommy skodje
*/
class FlatJsonDataSetConsumer( val strWriter: StringWriter )
extends IDataSetConsumer
{
  // val json	= new JSONObject()
  // Mapped convention
  val con = new MappedNamespaceConvention();
  val w = new MappedXMLStreamWriter(con, strWriter);
  // BadgerFish convention
  // AbstractXMLStreamWriter w = new BadgerFishXMLStreamWriter(strWriter);

  var currTable: ITableMetaData = null;


  def row(values: Array[AnyRef]) {
    w.writeStartElement( currTable.getTableName() )
    val m = currTable.getColumns map (_.getColumnName) zip values toMap;
    m.foreach( e => column( e._1, e._2 ) );
    w.writeEndElement()
  }

  private def column( key: String, value: AnyRef ) {
    val valS  = toString( value )
    if ( valS != null )
    	w.writeAttribute( key, valS );
    /*
    w.writeStartElement( key )
    if ( value != null )
      w.writeCharacters( value.toString )
    w. writeEndElement()
     */
  }
  private def toString( value: AnyRef ): String = { if ( value != null ) value.toString() else null };

  def endTable() { currTable = null; }

  def startTable(metaData: ITableMetaData) { currTable = metaData; }

  def endDataSet() {
    w.writeEndDocument()
    w.close();
    strWriter.close();
    System.out.println(strWriter.toString());
  }

  def startDataSet() { w.writeStartDocument() }
}