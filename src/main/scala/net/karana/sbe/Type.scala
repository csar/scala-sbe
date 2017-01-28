/*
 * Copyright year author
 */
package net.karana.sbe

import scala.xml.Node

object Type { //          semantic,   encoding, primitive
  val int8 = Type("int8",
                  Some(SemanticType.int),
                  Some("Byte"),
                  Some("Byte"),
                  Some(Presence.required),
                  0,
                  Some(1),
                  Some("-128"))
  val int16 = Type("int16",
                   Some(SemanticType.int),
                   Some("Short"),
                   Some("Short"),
                   Some(Presence.required),
                   0,
                   Some(2),
                   Some("-32768"))
  val int32 = Type("int32",
                   Some(SemanticType.int),
                   Some("Int"),
                   Some("Int"),
                   Some(Presence.required),
                   0,
                   Some(4),
                   Some(java.lang.Integer.MIN_VALUE.toString()))
  val int64 = Type("int64",
                   Some(SemanticType.int),
                   Some("Long"),
                   Some("Long"),
                   Some(Presence.required),
                   0,
                   Some(8),
                   Some(java.lang.Long.MIN_VALUE.toString()))
  val uint8 = Type("uint8",
                   Some(SemanticType.int),
                   Some("Byte"),
                   Some("UByte"),
                   Some(Presence.required),
                   0,
                   Some(1),
                   Some("-1"))
  val uint16 = Type("uint16",
                    Some(SemanticType.int),
                    Some("Short"),
                    Some("UShort"),
                    Some(Presence.required),
                    0,
                    Some(2),
                    Some("-1"))
  val uint32 = Type("uint32",
                    Some(SemanticType.int),
                    Some("Int"),
                    Some("UInt"),
                    Some(Presence.required),
                    0,
                    Some(4),
                    Some("-1"))
  val uint64 = Type("uint64",
                    Some(SemanticType.int),
                    Some("Long"),
                    Some("ULong"),
                    Some(Presence.required),
                    0,
                    Some(8),
                    Some("-1"))
  val ascii = Type("char",
                   Some(SemanticType.char),
                   Some("Char"),
                   Some("Char"),
                   Some(Presence.required),
                   0,
                   Some(1),
                   None)
  val float = Type("Float",
                   Some(SemanticType.float),
                   Some("Float"),
                   Some("Float"),
                   Some(Presence.required),
                   0,
                   Some(4),
                   None)
  val double = Type("Double",
                    Some(SemanticType.float),
                    Some("Double"),
                    Some("Double"),
                    Some(Presence.required),
                    0,
                    Some(8),
                    None)
  var encodingTypes: Map[String, Type] = Map("int8" -> int8,
                                             "uint8"  -> uint8,
                                             "int16"  -> int16,
                                             "uint16" -> uint16,
                                             "int32"  -> int32,
                                             "uint32" -> uint32,
                                             "int64"  -> int64,
                                             "uint64" -> uint64,
                                             "char"   -> ascii,
                                             "Float"  -> float)
  def apply(n: Node, register: Boolean): Type =
    attribute(n, "type") match {

      case Some(name) =>
        val typ          = encodingTypes(name)
        val offset       = attribute(n, "offset").map(_.toInt)
        val semanticType = SemanticType(attribute(n, "semanticType"))
        val since        = attribute(n, "sinceVersion").map(_.toInt).getOrElse(0) //typ.since
        val st           = if (semanticType.isDefined) semanticType else typ.semanticType
        if (st == Some(name) || typ.semanticType == Some(SemanticType.enum))
          typ.copy(offset = offset, since = since).complete()
        else
          typ
            .copy(semanticType = st, offset = offset, since = since)
            .complete()
      case None =>
        val name = attribute(n, "name").get
        val primitiveType =
          if (n.label == "enum") Some(name) else attribute(n, "primitiveType")
        val dt = attribute(n, "dimensionType")
        val encodingType =
          List(attribute(n, "encodingType"), dt).flatten.headOption
        val semanticType =
          if (n.label == "enum") Some(SemanticType.enum)
          else if (dt.isDefined) Some(SemanticType.NumInGroup)
          else SemanticType(attribute(n, "semanticType"))
        val presence = attribute(n, "presence").map { p =>
          Presence.withName(p)
        }
        val length =
          if (presence == Some(Presence.constant)) Some(0)
          else {
            attribute(n, "length") match {
              case Some(len) => Some(len.toInt)
              case None      => attribute(n, "blockLength").map(_.toInt)
            }
          }
        val content =
          if (presence == Some(Presence.constant))
            Some(n.child.head.toString())
          else
            None
        val nullValue = attribute(n, "nullValue")
        val composition =
          if (presence == Some(Presence.constant)) Nil
          else
            n.child
              .filter(attribute(_, "name").isDefined)
              .map(this.apply(_, false))
        val typ = Type(name,
                       semanticType,
                       encodingType,
                       primitiveType,
                       presence,
                       attribute(n, "sinceVersion").map(_.toInt).getOrElse(0),
                       length,
                       nullValue,
                       composition.toList,
                       content,
                       offset = attribute(n, "offset").map(_.toInt))
        if (register) encodingTypes += name -> typ

        typ
    }
}

/**
  * semanticType: General form defines the actual parsing
  * encodingType: This is what we read from the wire protocol
  * primitiveType: What we actually expose
  * composition: if no primitive type, composition is there, in this case the semanticType defines the actual primitive
  */
case class Type(name: String,
                semanticType: Option[SemanticType],
                encodingType: Option[String],
                primitiveType: Option[String],
                presence: Option[Presence.Presence],
                since: Int = 0,
                length: Option[Int],
                nullValue: Option[String],
                composition: List[Type] = Nil,
                content: Option[String] = None,
                isFix: Boolean = false,
                offset: Option[Int] = None) {
  def parseLength: Int =
    if (isConstant) 0
    else if (length.getOrElse(0) > 0) length.get
    else if (encodingType.isDefined) {
      Type.encodingTypes(encodingType.get).complete().parseLength
    } else {
      composition.map(_.parseLength).sum
    }
  def variableLength = semanticType match {
    case Some(SemanticType.NumInGroup) => true
    case _                             => false
  }
  def optional = presence == Some(Presence.optional)
  def isConstant: Boolean = presence match {
    case Some(presence) => presence == Presence.constant
    case None if composition.isEmpty =>
      try {
        val typ = primitiveType.get
        if (typ == name)
          false
        Type
          .encodingTypes(typ)
          .isConstant // root types are always defaulted with required
      } catch {
        case nse: NoSuchElementException =>
          throw new Error(toString)
      }
    case _ => false
  }
  def constant: String =
    content.getOrElse(Type.encodingTypes(primitiveType.get).constant)
  def isFixType() = isFix
  //    case "String"|"Char"|"Int"|"Short"|"Byte"|"Long"|"BigDecimal" => false
  //    case _ => true
  //  }

  def encoded = encodingType.flatMap(Type.encodingTypes.get(_))
  def mandatory: Boolean =
    semanticType.map(_.isInstanceOf[AlwaysPresent]).getOrElse(false) ||
    (presence match {
      case Some(p) => p != Presence.optional
      case None => //
        composition.forall(_.mandatory) &&
        encoded.map(_.mandatory).getOrElse(true)
    })
  def isNamedConstant =
    Type.encodingTypes(name).presence == Some(Presence.constant)
  def fixType: String = semanticType.get match {
    case SemanticType.MultipleCharValue => name
    case _ if isNamedConstant           => name
    case s: StringSemantics             => if (length == Some(1)) "Char" else "String"
    case _ =>
      primitiveType match {
        case Some(t) => t
        case None    => semanticType.get.primitive(this)
      }
  }
  //  def resultType: Option[String] = semanticType match {
  //    case Some(sem) => semanticType
  //    case None => if (primitiveType.isDefined) Type.encodingTypes(primitiveType.get).resultType else
  //      None
  //  }
  private def isParsable = encodingType.getOrElse("") match {

    case "Byte" | "Short" | "Char" | "Int" | "Long" | "String" | "BigDecimal" |
        "Float" | "Double" | "Boolean" =>
      true
    case _ => false
  }
  private def isEncoded = primitiveType.getOrElse("") match {
    case "Byte" | "Short" | "Char" | "Int" | "Long" | "String" | "BigDecimal" |
        "Float" | "Double" | "Boolean" =>
      true
    case "UByte" | "UShort" | "UInt" | "ULong" => true
    case _                                     => false
  }

  def merge(parent: Type) = {
    var changed   = false
    var completed = this

    if (parent.encodingType.isDefined && completed.encodingType != parent.encodingType) {
      changed = true
      completed = completed.copy(encodingType = parent.encodingType)
    }
    if (parent.length.isDefined && completed.length.isEmpty) {
      changed = true
      completed = completed.copy(length = parent.length)
    }
    if (parent.nullValue.isDefined && completed.nullValue.isEmpty) {
      changed = true
      completed = completed.copy(nullValue = parent.nullValue)
    }
    if (completed.semanticType.isEmpty) {
      changed = true
      completed = completed.copy(semanticType = parent.semanticType)
    }
    if (parent.content.isDefined && completed.content.isEmpty) {
      changed = true
      completed = completed.copy(content = parent.content)
    }
    if (parent.presence.isDefined && completed.presence.isEmpty) {
      changed = true
      completed = completed.copy(presence = parent.presence)
    }
    if (!parent.composition.isEmpty && completed.composition.isEmpty) {
      changed = true
      completed = completed.copy(composition = parent.composition)
    }

    completed = completed.copy(primitiveType = parent.primitiveType)
    if (changed) completed.complete()
    else completed
  }
  def complete(): Type =
    //Console.err.println(this)
    if (isParsable && isEncoded) {
//      println(this)
      this
    } else {
      var completed = this
      if (!isEncoded && encodingType.isDefined) {
        completed = completed.merge(Type.encodingTypes(encodingType.get))
      }
      if (!isParsable && primitiveType.isDefined) {
        completed = completed.merge(Type.encodingTypes(primitiveType.get))
      }

      completed

    }

  //  def underlyingType: String = encodingType match {
  //    case Some(tn) => Type.encodingTypes(tn).underlyingType
  //    case None     => primitiveType.get
  //  }
}
