/* FileUtil.scala
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
package no.antares.util

import java.io._
import java.net.{URL, URI}

/** @author Tommy Skodje */

object FileUtil {
  /** Is this a file-based url?.	*/
  def isFile(url: URL): Boolean = {
    if (url == null) return false
    return url.toString.startsWith("file:/") || url.toString.startsWith("jar:")
  }

  /** Get file from file-based url.	*/
  def fromFileUrl( url: URL ): File = {
    if ( ! isFile(url))
      return null

    if (url.toString.startsWith("jar:file:"))
      return fromFileUrl( new URL( url.toString.substring(4) ) )

    try {
      return new File( new URI( url.toString ).getPath )
    } catch {
      case e: Exception => throwon( "Unbelievable - Exception in getFile( " + url + " )\n", e );
    }
  }

  /**  */
  def getFromClassPath( file: String ): File = {
    fromFileUrl( getClass.getClassLoader.getResource( file ) )
  }

  /**  */
  def getFromWorkingDir( file: String ): File = new File( file )

  /** Read from an input stream and stuff into the output stream */
  def streamData(in: Reader, out: Writer): Unit = {
    if ((in != null) && (out != null)) {
      var buf: Array[Char] = new Array[Char](4 * 1024)
      var bytesRead: Int = 0
      while ((({
        bytesRead = in.read(buf); bytesRead
      })) != -1) {
        out.write(buf, 0, bytesRead)
      }
    }
  }

  /**Read the file and return contents in String */
  def readUTF8File(file: File): String = {
    if (file == null) return null
    var fReader: Reader = null
    var sw: CharArrayWriter = null
    try {
      if (!file.canRead) return null
      fReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
      sw = new CharArrayWriter
      streamData(fReader, sw)
      return sw.toString
    }
    catch {
      case e: Exception => {
        var message: String = "Exception in readUTF8File(" + file.getAbsolutePath + ")\n"
        throw new RuntimeException(message, e)
      }
    }
    finally {
      catchall( () => { fReader.close() } )
      catchall( () => { sw.close() } )
    }
  }

  def catchall( f : () => Unit ): Unit = {
    try {
       f()
    } catch {
      case t: Throwable => ;	// ignore
    }
  }

  def throwon( msg: String, t: Throwable ): Nothing = throw new RuntimeException( msg, t );

}