package no.antares.dbunit

import org.dbunit.dataset.stream.IDataSetConsumer
import org.dbunit.dataset.ITableMetaData
import org.codehaus.jettison.json.JSONObject
import org.codehaus.jettison.AbstractXMLStreamWriter
import org.codehaus.jettison.mapped.{MappedNamespaceConvention, MappedXMLStreamWriter}
import java.io.StringWriter

/** @author tommyskodje */

/** Produces a dbUnit dataset from json, mirrors dbUnit FlatXmlProducer.

I chose to reimplement most of FlatXmlProducer here
I could have extended only produce() method in FlatXmlProducer, and use jettison XmlStreamReader
org.springframework.util.xml.StaxStreamXMLReader converts from XmlStreamReader to XmlReader

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
    var currIdx = 0;
    // val map = values.foldLeft(Map[Int,AnyRef]()) { (m, s) => m(currIdx) = s }
    // val string2Length = Map(values map {s => ( currIdx , s )} : _*)
    val m = currTable.getColumns map (_.getColumnName) zip values toMap;
    m.foreach( e => entry( e._1, e._2 ) );
  }

  private def entry( key: String, value: AnyRef ) {
    w.writeStartElement( key )
    if ( value != null )
      w.writeCharacters( value.toString )
    w.writeEndElement()
  }

  def endTable() { w.writeEndElement() }

  def startTable(metaData: ITableMetaData) {
    currTable = metaData;
    w.writeStartElement( metaData.getTableName() )
  }

  def endDataSet() {
    w.writeEndDocument()
    w.close();
    strWriter.close();
    System.out.println(strWriter.toString());
  }

  def startDataSet() { w.writeStartDocument() }
}