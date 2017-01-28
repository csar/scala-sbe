/*
 * Copyright year author
 */
package net.karana.sbe

import scala.xml.XML
import java.io.File
import java.io.FileOutputStream
import java.io.FileWriter
import scala.xml.Node
import scala.xml.Elem
import java.nio.ByteBuffer

object SBE extends App {

  val outPath = new File("../cme/src/main/scala/sbe/cme/types")
  var types   = Set.empty[Node]
  outPath.mkdirs()
  val sbeNS         = "http://www.fixprotocol.org/ns/simple/1.0"
  val modifier      = "" //"private "
  val schemaVersion = 8
  val schema        = XML.loadFile("templates_FixBinary.xml")
  val out           = new File(outPath, "MarktData.scala")
  val fo            = new FileWriter(out)
  fo write "package sbe.cme.types"
  fo write eol
  fo write """import java.nio.ByteBuffer
    import java.nio.charset.StandardCharsets
    import net.karana.unsigned.UByte
    import net.karana.unsigned.UShort
    import net.karana.unsigned.UInt
    import net.karana.unsigned.ULong
    import collection.mutable.ListBuffer
    import scala.util.Try
"""
  fo write eol
  fo write """trait Parser {
  def parse(b: ByteBuffer, header: MessageHeader): Message
}
trait Writable
case class MessageHeader(blockLength: Int, templateId: Int, schemaId: Int, version: Int)
"""
  fo write "trait Message{def write(b: ByteBuffer)}" + eol
  fo write
  modifier + """ object Helper {
  def bitSet[T<:AnyRef](s:Byte, constructor : (Int)=>T) : Set[T] = {
    var bits = Set.empty[T]
    var pos = 0
    var remain = s & 0xff
    while(remain!=0) {
      if ( (remain&1) == 1) bits+= constructor(pos)
      pos+=1
      remain = remain >>> 1
    }
    bits
  }
  def slice(b:ByteBuffer):ByteBuffer = {
    val copy = b.slice()
    copy.order(b.order())
    copy
  }
  def add(parent:ByteBuffer,child:ByteBuffer) {
    parent.position(parent.position()+child.position())
  }
  def put(b: ByteBuffer, obj: ULong): Unit  = b.putLong(obj.long)
  def put(b: ByteBuffer, obj: UInt): Unit   = b.putInt(obj.int)
  def put(b: ByteBuffer, obj: UShort): Unit = b.putShort(obj.short)
  def put(b: ByteBuffer, obj: UByte): Unit  = b.put(obj.byte)
  def put(b: ByteBuffer, obj: Long): Unit   = b.putLong(obj)
  def put(b: ByteBuffer, obj: Int): Unit    = b.putInt(obj)
  def put(b: ByteBuffer, obj: Short): Unit  = b.putShort(obj)
  def put(b: ByteBuffer, obj: Byte): Unit   = b.put(obj)
  def put(b: ByteBuffer, obj: Char): Unit   = b.put(obj.toByte)
  def put(b: ByteBuffer, obj: Double): Unit = b.putDouble(obj)
  def put(b: ByteBuffer, obj: Float): Unit  = b.putFloat(obj)
  def put(b: ByteBuffer, obj: String, len: Int): Unit = {
    val ascii = obj.getBytes(StandardCharsets.US_ASCII)
    b.put(ascii, 0, ascii.length min len)
    fill(b,len - ascii.length)
  }
  def fill(b:ByteBuffer, n:Int) = {
    val zero:Byte=0
    var zeros = n max 0
    while (zeros > 0) {
      b.put(zero)
      zeros -= 1
    }
  }
  def skip(b:ByteBuffer, n:Int) = {
      b.position(b.position+n)
  }
  def parseUByte(b:ByteBuffer):UByte = b.get
  def parseUShort(b:ByteBuffer):UShort = b.getShort
  def parseUInt(b:ByteBuffer):UInt = b.getInt
  def parseULong(b:ByteBuffer):ULong = b.getLong
  def parseByte(b:ByteBuffer):Byte = b.get
  def parseShort(b:ByteBuffer):Short = b.getShort
  def parseInt(b:ByteBuffer):Int = b.getInt
  def parseLong(b:ByteBuffer):Long = b.getLong
  def parseChar(b:ByteBuffer):Char = b.get.toChar
  def parseUByte(b:ByteBuffer, nullValue:Byte):Option[UByte] = {
    val read = b.get
    if (read == nullValue) None
    else Some(read)
  }
  def parseUShort(b:ByteBuffer, nullValue:Short):Option[UShort] = {
    val read = b.getShort
    if (read == nullValue) None
    else Some(read)
  }
  def parseUInt(b:ByteBuffer, nullValue:Int):Option[UInt] = {
    val read = b.getInt
    if (read == nullValue) None
    else Some(read)
  }
  def parseULong(b:ByteBuffer, nullValue:Long):Option[ULong] = {
    val read = b.getLong
    if (read == nullValue) None
    else Some(read)
  }
  def parseByte(b:ByteBuffer, nullValue:Byte):Option[Byte] = {
    val read = b.get
    if (read == nullValue) None
    else Some(read)
  }
  def parseShort(b:ByteBuffer, nullValue:Short):Option[Short] = {
    val read = b.getShort
    if (read == nullValue) None
    else Some(read)
  }
  def parseInt(b:ByteBuffer, nullValue:Int):Option[Int] = {
    val read = b.getInt
    if (read == nullValue) None
    else Some(read)
  }
  def parseLong(b:ByteBuffer, nullValue:Long):Option[Long] = {
    val read = b.getLong
    if (read == nullValue) None
    else Some(read)
  }
  def parseChar(b:ByteBuffer, nullValue:Byte):Option[Char] = {
    val read = b.get
    if (read == nullValue) None
    else Some(read.toChar)
  }
  def parseString(b:ByteBuffer, len:Int):String = {
    var i=0
    val bytes = new Array[Byte](len)
    while(i<len) {
      val byte = b.get
      if (byte==0) {
        skip(b,len-i-1)
        return new String(bytes,0,i,StandardCharsets.US_ASCII)
      }
      else {
        bytes(i) = byte
        i+=1
      }
    }
    new String(bytes, StandardCharsets.US_ASCII)
  }
}""" + eol
  //types
  //Type(name,semanticType,encodingType,primitiveType,presence,length, nullValue, composition.toList, content)

  for (types <- schema \ "types") {
    for (typ <- types \ "type"
         if attribute(typ, "presence") != Some("constant")) {
      val name = attribute(typ, "name").get
      //      val semanticType = attribute(typ, "semanticType").flatMap(t=>encodingType.get(t)) match {
      //        case Some(t) => t.semanticType
      //        case None => attribute(typ, "primitiveType").flatMap(t=>encodingType.get(t)) match {
      //          case Some(t) => t.semanticType
      //          case None => ""
      //        }
      //      }
      val semanticType = attribute(typ, "semanticType").getOrElse("")
      Type(typ, true)
    }
    for (typ <- types \ "composite") {
      Type(typ, true)
    }
    fo write "object Types {" + eol
    //    for ((name, atyp) <- Type.encodingTypes if (atyp.composition.isEmpty); typ = atyp.complete() if name != typ.semanticType.getOrElse(name)) {
    //      fo write "type " + name + " = " + typ.semanticType.get + eol
    //    }
    fo write "case class MonthYear(year:Option[UShort],month:Option[UByte],week:Option[UByte],day:Option[UByte])" + eol
    //    fo write "case class Exchange(name:String)" + eol
    //    fo write "case class Currency(name:String)" + eol
    //    fo write "case class Qty(name:String)" + eol
    //    fo write "case class NumInGroup(name:String)" + eol
    //    fo write "case class LocalMktDate(date:Int)" + eol
    fo write """trait CharValue {
    val toChar:Char
  }""" + eol
    fo write """trait ByteValue {
    val toByte:Byte
  }""" + eol
    fo write """trait ShortValue {
    val toShort:Short
  }""" + eol
    fo write """trait IntValue {
    val toInt:Int
  }""" + eol
    fo write """trait LongValue {
    val toLong:Long
  }""" + eol
    fo write "}" + eol
    fo write "import Types._" + eol

    //constants
    for (typ <- types \ "type"
         if attribute(typ, "presence") == Some("constant")) {
      val name = attribute(typ, "name").get
      val resultType = Type
        .encodingTypes(attribute(typ, "primitiveType").get)
        .complete
        .primitiveType
      fo write "sealed trait " + name + " extends " + resultType.get + "Value" + eol
      fo write "object " + name + " {" + eol

      val (open, close) = resultType.get match {
        case "Char"   => ("'", "'")
        case "String" => ("\"", "\"")
        case "Byte"   => ("", ".toByte")
        case "Int"    => ("", "")
      }
      val disc = typ.child.head.toString
      fo write "case object " + name + " extends " + name + " { val to" + resultType.get + " = " + open + disc + close + " }" + eol

      fo write "}" + eol
      fo write "import " + name + "._" + eol
      Type(typ, true)
    }
    // enums
    // sets
    for (set <- types \ "set") {

      val name = attribute(set, "name").get

      fo write "object " + name + " extends Enumeration {" + eol
      fo write "type " + name + " = Value" + eol
      val values = for (enum <- set \ "choice") yield {
        val name  = attribute(enum, "name").get
        val place = enum.child.head.toString().toInt
        (place -> name)
      }
      var map = values.toMap
      var id  = 0
      fo write "val "
      while (!map.isEmpty) {
        fo write map.getOrElse(id, "_" + id)
        map -= id
        id += 1
        if (!map.isEmpty) fo write ", "

      }
      fo write " = Value" + eol
      fo write "}" + eol
      fo write "import " + name + "._" + eol
      Type(set, true)
    }

    //enumeratives
    for (set <- types \ "enum") {
      val name       = attribute(set, "name").get
      val typ        = Type.encodingTypes(attribute(set, "encodingType").get)
      val resultType = typ.complete()
      fo write "sealed trait " + name + " extends " + resultType.encodingType.get + "Value" + eol
      fo write "object " + name + " {" + eol

      val (open, close) = resultType.encodingType.get match {
        case "Char"   => ("'", "'")
        case "String" => ("\"", "\"")
        case "Byte"   => ("", ".toByte")
        case "Int"    => ("", "")
        case _ =>
          throw new Error("strange primitive")
      }
      for (enum <- set \ "validValue") {
        val value = attribute(enum, "name").get
        val disc  = enum.child.head.toString
        fo write "case object " + value + " extends " + name + " { val to" + resultType.encodingType.get + " = " + open + disc + close + " }" + eol

      }
      val c =
        if (resultType.encodingType == typ.primitiveType) "c"
        else "c.to" + resultType.encodingType.get
      fo write "def apply(c:" + resultType.primitiveType.get + ") = " + c + " match {" + eol
      for (enum <- set \ "validValue") {
        val value = attribute(enum, "name").get
        val disc  = enum.child.head.toString
        fo write "case " + open + disc + open + " => " + value + eol
      }
      fo write "}" + eol
      fo write "}" + eol
      fo write "import " + name + "._" + eol
      Type(set, true)
    }
  }

  //messages
  val parseables = for (x <- schema \ "message" if x.namespace == sbeNS)
    yield {
      createType(x, x.attribute("description").get.toString)
      x
    }
  fo write """object Parser {
  def header(ob:ByteBuffer) = {
    val b = Helper.slice(ob) 
    val blockLength: UShort = b.getShort
    val templateId: UShort = b.getShort
    val schemaId: UShort = b.getShort
    val version: UShort = b.getShort
    MessageHeader(blockLength.toInt, templateId.toInt, schemaId.toInt, version.toInt)
  }
  def parser(header: MessageHeader) = messages.get(header.templateId)
  def parse(header: MessageHeader,b: ByteBuffer) = Try{
      parser(header).get.
        parse(Helper.slice(b), header)
    }
  def parse(b: ByteBuffer): Try[Message] = {
    val header =  this.header(b)
    val positionedBuffer =  Helper.slice(b)
    Helper.skip(positionedBuffer,8)
    parse(header,positionedBuffer)
  }

  val messages:Map[Int, Parser] ="""

  val map = for (x <- parseables; name = attribute(x, "description");
                 id = attribute(x, "id"); templateId <- id; obj <- name)
    yield {
      s"$templateId -> $obj"

    }
  fo write map.mkString("Map(", "," + eol, ")" + eol + "}") + eol

  def createType(x: Node, name: String, header: Boolean = true): Unit = {
    val (signature, newtypes) = fields(x, name + ".")

    fo write "case class " + name + "("

    fo write (s"version : Int =$schemaVersion)" :: (signature.toList.reverse)).reverse
      .mkString("," + eol)
    def serialize: String = {
      val sb = new StringBuilder
      val allFieldnames = signature.map { s =>
        s.split(':')(0)
      }
      var inRootBlock = header
      if (!header)
        sb append "val version = messageVersion min this.version" + eol
      var offset = 0
      for (field <- allMembers(x)) {
        val fieldName = attribute(field, "name").get
        val typ       = Type(field, false).complete()
        if (typ.since > 0)
          println(">" + fieldName)

        for (offoverride <- typ.offset if offoverride > offset) {
          // writing zeros
          sb append s"Helper.fill(b, ${offoverride - offset})$eol"
          offset = offoverride
        }
        if (typ.variableLength) {
          if (inRootBlock) {
            // end the rootblock parse
            sb append s"Helper.fill(b,${attribute(x, "blockLength").get.toInt + 8}-b.position)" + eol
          }
          inRootBlock = false
          val (nono, singular) = computeSingleName(fieldName)
          val str =
            typ.semanticType.get.write(typ, nono + ";" + typ.length.get)._1
          sb append str + eol
          sb append s"""$nono.foreach{ element =>
            val gb = Helper.slice(b)
            element.write(gb,version)
            Helper.fill(gb,${typ.length.get}-gb.position)
            Helper.add(b,gb)
          }""" + eol
        } else if (!typ.isConstant) {
          if (typ.name == "EventType") {
            println(typ)
          }
          val (str, len) = typ.semanticType.get.write(typ, fieldName)
          offset += len
          sb append str + eol
        }

      }
      sb.toString()
    }
    if (header) {
      fo write s""" extends Message{
  def write(b: ByteBuffer) {
    Helper.put(b,${attribute(x, "blockLength").get}.toShort)
    Helper.put(b,${attribute(x, "id").get}.toShort)
    Helper.put(b,1.toShort)
    Helper.put(b,version.toShort)
    $serialize
  }

}""" + eol
    } else {
      fo write s""" {
        def write(b: ByteBuffer, messageVersion:Int) {
          $serialize
        }
      }""" + eol

    }
    fo write eol
    fo write "object " + name + (if (header) " extends Parser {" else " {") + eol

    // compute all fields
    val sb = new StringBuilder
    val allFieldnames = signature.map { s =>
      s.split(':')(0)
    }
    var inRootBlock = true
    sb append "var gb:ByteBuffer = b" + eol
    var offset = 0
    for (field <- allMembers(x)) {
      val fieldName = attribute(field, "name").get
      val typ       = Type(field, false).complete()
      if (typ.since > 0)
        println(fieldName)

      for (offoverride <- typ.offset if offoverride > offset) {
        sb append s"gb.position($offoverride)$eol"
        offset = offoverride
      }
      if (typ.variableLength) {
        if (inRootBlock) {
          // end the rootblock parse
          sb append "gb.position(header.blockLength)" + eol
        }
        inRootBlock = false
        val versioned =
          if (typ.since > 0) Some(s"if(header.version >= ${typ.since})")
          else None

//        val group = Type.encodingTypes(attribute(field, "dimensionType").get).complete()
        val show = typ.semanticType.get.parse(typ)._1

//        val sizeStr = s"""val blockLength=Helper.parse${group.composition.head.complete().primitiveType.get}(gb).toInt
//          var numInGroup=Helper.parse${group.composition.tail.head.complete().primitiveType.get}(gb).toInt""" + eol
        val (nono, singular) = computeSingleName(fieldName)
        sb append s"""val _$nono = ${versioned.getOrElse("")}{ 
          $show
       var buffer = List.empty[$singular]
       while (numInGroup!=0) {
         val eb = Helper.slice(gb)
         buffer = $singular.parse(eb,header) :: buffer
         eb.position(blockLength)
         Helper.add(gb,eb)
         numInGroup -=1
       }
       buffer.reverse
     }""" + eol
        if (versioned.isDefined) sb append "else Nil"
        // parsing var length entities
      } else if (typ.isConstant) {
        // constant
        sb append s"val _$fieldName = ${typ.name}.${typ.name}" + eol
      } else {
        if (fieldName == "MinPriceIncrement") {
          println(typ)
        }
        val (str, len) = typ.semanticType.get.parse(typ)
        offset += len

        sb append s"val _$fieldName = " + str + eol
      }

    }
    val parsing = sb.toString()
    fo write s"""def parse(b:ByteBuffer, header:MessageHeader): $name = {
      $parsing
      $name(${("header.version" :: (allFieldnames
      .map("_" + _)
      .toList
      .reverse)).reverse.mkString(", " + eol)})
      }
      """
    for (typ <- newtypes) {
      createType(typ,
                 computeSingleName(typ.attribute("name").get.toString())._2,
                 false)
    }
    fo write "}" + eol
  }
  fo.close
  def allMembers(n: Node) =
    n.child.filter(c => c.label == "field" || c.label == "group")
  def fields(n: Node, container: String): (Seq[String], Seq[Node]) = {
    val fields = for (f <- n \ "field") yield {
      val typ = Type(f, false).complete()
      val sig =
        if (typ.semanticType == Some(SemanticType.MultipleCharValue))
          s"Set[${typ.fixType}]"
        else if (typ.mandatory) typ.fixType
        else s"Option[${typ.fixType}]"
      f.attribute("name").get + ": " + sig
    }
    val groups = for (f <- n \ "group") yield {
      val (fn, typeName) = computeSingleName(
        f.attribute("name").get.toString())
      (fn + ":List[" + container + typeName + "]", f)
    }
    ((fields ++ groups.map(_._1)), groups.map(_._2))
  }
  def computeSingleName(name: String): (String, String) = {
    val nono =
      if (name.startsWith("No") && name.charAt(2).isUpper) name.substring(2)
      else name
    val singular = if (nono.endsWith("ies")) {
      nono.substring(0, nono.length() - 3) + "y"
    } else if (nono.endsWith("s")) {
      nono.substring(0, nono.length() - 1)
    } else nono

    (nono, singular)
  }
}

object Presence extends Enumeration {
  type Presence = Value
  val required, optional, constant = Value
}

// end of semantic types
