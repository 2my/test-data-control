/*  Found code at http://www.java2s.com/Code/Java/XML/XmlReaderToWriter.htm
 *   Copyright 2004 The Apache Software Foundation
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *  limitations under the License.
*/
package no.antares.xstream

import javax.xml.stream.{XMLStreamConstants, XMLStreamReader, XMLStreamWriter}

/** Streams from xml reader to writer. */
object XmlStreamer {
  def writeAll(xmlr: XMLStreamReader, writer: XMLStreamWriter): Unit = {
    while (xmlr.hasNext) {
      write(xmlr, writer)
      xmlr.next
    }
    write(xmlr, writer)
    writer.flush
  }

  def write(xmlr: XMLStreamReader, writer: XMLStreamWriter): Unit = {
    xmlr.getEventType match {
      case XMLStreamConstants.START_ELEMENT =>
        val localName: String = xmlr.getLocalName
        val namespaceURI: String = xmlr.getNamespaceURI
        if (namespaceURI != null && namespaceURI.length > 0) {
          val prefix: String = xmlr.getPrefix
          if (prefix != null)
            writer.writeStartElement(prefix, localName, namespaceURI)
          else
            writer.writeStartElement(namespaceURI, localName)
        } else {
          writer.writeStartElement(localName)
        }

        var i: Int = 0
        val nsCount = xmlr.getNamespaceCount
        while (i < nsCount) {
          writer.writeNamespace(xmlr.getNamespacePrefix(i), xmlr.getNamespaceURI(i))
          i += 1;
        }

        i = 0
        val attrCount = xmlr.getAttributeCount
        while (i < attrCount) {
          var attUri: String = xmlr.getAttributeNamespace(i)
          if (attUri != null)
            writer.writeAttribute(attUri, xmlr.getAttributeLocalName(i), xmlr.getAttributeValue(i))
          else
            writer.writeAttribute(xmlr.getAttributeLocalName(i), xmlr.getAttributeValue(i))
          i += 1;
        }
        return;
      case XMLStreamConstants.END_ELEMENT =>
        writer.writeEndElement
        return;
      case XMLStreamConstants.SPACE =>
      case XMLStreamConstants.CHARACTERS =>
        writer.writeCharacters(xmlr.getTextCharacters, xmlr.getTextStart, xmlr.getTextLength)
        return;
      case XMLStreamConstants.PROCESSING_INSTRUCTION =>
        writer.writeProcessingInstruction(xmlr.getPITarget, xmlr.getPIData)
        return;
      case XMLStreamConstants.CDATA =>
        writer.writeCData(xmlr.getText)
        return;
      case XMLStreamConstants.COMMENT =>
        writer.writeComment(xmlr.getText)
        return;
      case XMLStreamConstants.ENTITY_REFERENCE =>
        writer.writeEntityRef(xmlr.getLocalName)
        return;
      case XMLStreamConstants.START_DOCUMENT =>
        var encoding: String = xmlr.getCharacterEncodingScheme
        var version: String = xmlr.getVersion
        if (encoding != null && version != null) writer.writeStartDocument(encoding, version)
        else if (version != null) writer.writeStartDocument(xmlr.getVersion)
        return;
      case XMLStreamConstants.END_DOCUMENT =>
        writer.writeEndDocument
        return;
      case XMLStreamConstants.DTD =>
        writer.writeDTD(xmlr.getText)
        return;
    }
  }
}