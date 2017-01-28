/*
 * Copyright year author
 */
package net.karana
import java.io.File
import scala.xml.Node

package object sbe {

  def getFieldByName(x: Node, name: String) = {
    val fields = (x \ "field")
    fields.filter { f =>
      attribute(f, "name") == Some(name)
    }.headOption match {
      case Some(field) => field
      case None =>
        null
    }
  }
  def attribute(n: Node, name: String) = n.attribute(name).map(_.toString)
  def eol                              = System.lineSeparator()
}
