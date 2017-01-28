/*
 * Copyright year author
 */
package net.karana.sbe

sealed trait SemanticType {
  def putNone(typ: Type, bufferName: String = "b") =
    s"Helper.put($bufferName,${nullValue(typ)}.to${typ.encodingType.get})"
  def since(typ: Type) =
    if (typ.since == 0) None else Some(s"if(header.version >= ${typ.since}) ")
  def primitive(typ: Type): String
  def write(typ: Type,
            fieldName: String,
            bufferName: String = "b"): (String, Int)
  //  = (if (typ.optional)
  //    s"if($fieldName.isEmpty) Helper.put($bufferName,${typ.parseLength}) else Helper.put($bufferName,$fieldName.get)"
  //  else s"Helper.put(b,$fieldName)", typ.parseLength)
  def constructor(typ: Type, arg: Option[String]) =
    typ.primitiveType.get match {
      case "Int"    => arg.getOrElse("0")
      case "Short"  => arg.getOrElse("0") + ".toShort"
      case "Byte"   => arg.getOrElse("0") + ".toByte"
      case "Long"   => arg.getOrElse("0") + "L"
      case "Float"  => arg.getOrElse("0") + "F"
      case "Double" => arg.getOrElse("0") + "D"
      case "String" => "\"" + arg.getOrElse("") + "\""
      case "Char"   => "'" + arg.getOrElse("u/0000") + "'"
      case "UByte"  => s"UByte(${arg.getOrElse("0")}.toByte)"
      case "UShort" => s"UShort(${arg.getOrElse("0")}.toShort)"
      case "UInt"   => s"UInt(${arg.getOrElse("0")})"
      case "ULong"  => s"ULong(${arg.getOrElse("0")}L)"
    }
  def nullValue(typ: Type) =
    try {
      val lnv = BigInt(typ.nullValue.get)
      val nnv = typ.encodingType match {
        case Some("Byte")  => lnv.toByte.toString()
        case Some("Short") => lnv.toShort.toString()
        case Some("Int")   => lnv.toInt.toString()
        case Some("Long")  => lnv.toLong.toString() + "L"
      }
      nnv
    } catch {
      case nfe: NumberFormatException =>
        typ.nullValue.get
    }
  def parse(typ: Type): (String, Int)
}
trait OptValue { self: SemanticType =>
  def optValue(typ: Type): String =
    if (typ.presence == Some(Presence.optional)) {
      val lnv = BigInt(typ.nullValue.get)
      val nnv = typ.encodingType match {
        case Some("Byte")  => lnv.toByte.toString()
        case Some("Short") => lnv.toShort.toString()
        case Some("Int")   => lnv.toInt.toString()
        case Some("Long")  => lnv.toLong.toString()
        case Some(x)       => optValue(Type.encodingTypes(x).complete())
      }
      s", $nnv${if (typ.length == Some(8)) "L" else ""}"
    } else ""

}
trait NumericSemantics extends OptValue { self: SemanticType =>
  def primitive(typ: Type): String = typ.primitiveType match {
    case Some(pn) => pn
    case None     => "BigDecimal"
  }
  override def write(typ: Type,
                     fieldName: String,
                     bufferName: String): (String, Int) =
    if (primitive(typ) == "BigDecimal") {
      val sb                            = new StringBuilder
      val options                       = typ.composition.map(_.complete())
      val opts                          = options.exists(_.optional)
      val (mantissa :: exponent :: Nil) = options
      if (exponent.isConstant) {
        // we only write the long
        val exp = exponent.content.get.toInt
        if (opts) {
          sb append s"if($fieldName.isEmpty) ${putNone(mantissa)} else Helper.put($bufferName,$fieldName.get.bigDecimal.movePointLeft($exp).${mantissa.encodingType.get.toLowerCase()}Value())"
        } else
          sb append s"Helper.put($bufferName,$fieldName.bigDecimal.movePointLeft($exp).${mantissa.encodingType.get.toLowerCase()}Value())"
      } else {
        throw new Error("undefined exp")
      }
      (sb.toString(), mantissa.parseLength)
    } else if (typ.optional)
      (s"if($fieldName.isEmpty) ${putNone(typ)} else Helper.put(b,$fieldName.get)",
       typ.parseLength)
    else
      (s"Helper.put(b,$fieldName)", typ.parseLength)
  def parse(typ: Type): (String, Int) = {
    val sb = new StringBuffer()
    for (cond <- since(typ)) {
      sb append cond
      println("--handled")
    }

    val len = if (primitive(typ) == "BigDecimal") {
      sb append "{" + eol
      var decLen = 0
      val options = for (tc <- typ.composition; t = tc.complete()) yield {
        sb append "val _" append t.name append " = "
        val (str, len) = t.semanticType.get.parse(t)
        sb append str append eol
        decLen += len
        if (t.presence == Some(Presence.optional)) Some('_' + t.name)
        else None
      }
      val opts                          = options.flatten
      val (mantissa :: exponent :: Nil) = typ.composition.map(_.name)
      val invoke                        = s"BigDecimal(_$mantissa, - _$exponent)"
      if (opts.isEmpty) sb append invoke
      else {

        def build(args: List[String]): Unit =
          if (args.isEmpty) {
            sb append s"Some($invoke)$eol"
          } else {
            val (first :: tail) = args
            sb append s"$first match { case Some($first) =>$eol"
            build(tail)
            sb append "case None => None" + eol + "}" + eol
          }
        build(opts)
      }
      sb append "}" + eol
      if (since(typ).isDefined) {
        sb append s"else { "
        if (opts.isEmpty) sb append "null "
        else sb append "None "
        "}+eol"
      }
      decLen
    } else if (typ.presence == Some(Presence.constant)) {
      sb append constructor(typ, typ.content)
      if (since(typ).isDefined) {
        if (typ.optional) sb append s" else None"
        else sb append s" else  ${constructor(typ, Some(nullValue(typ)))}"
      }
      0
    } else {

      sb append s"Helper.parse${typ.primitiveType.getOrElse("Int")}(gb${optValue(typ)})"
      if (since(typ).isDefined) {
        if (typ.optional) sb append s" else None"
        else sb append s" else  ${constructor(typ, Some(nullValue(typ)))}"

      }
      typ.parseLength
    }

    (sb.toString(), len)
  }

}
trait StringReadSemantics { self: SemanticType =>
  def primitive(typ: Type): String = toString
  def parse(typ: Type): (String, Int) = {
    val sb = new StringBuffer
    sb append since(typ).getOrElse("")
    val (str, len) = typ.length match {
      case Some(1) if typ.primitiveType == Some("Char") =>
        (s"Helper.parseChar(gb)", 1)
      case Some(len) =>
        (s"Helper.parse${typ.primitiveType.getOrElse("String")}(gb,$len)", len)
      case None =>
        (s"Helper.parse${typ.primitiveType.getOrElse("String")}(gb)",
         typ.parseLength)
    }
    sb append str
    if (since(typ).isDefined) {
      println("--handled")
      sb append " else "
      if (typ.optional) sb append "None"
      else sb append constructor(typ, typ.nullValue)
    }
    (sb.toString(), len)
  }
}
trait StringSemantics { self: SemanticType =>
  def primitive(typ: Type): String = ???
  override def write(typ: Type, fieldName: String, bufferName: String) =
    typ.length match {
      case Some(1) => (s"Helper.put(b,$fieldName)", 1)
      case Some(n) => (s"Helper.put(b,$fieldName,$n)", n)
    }
  def parse(typ: Type): (String, Int) = {
    val sb = new StringBuffer
    sb append since(typ).getOrElse("")
    val (str, len) = typ.length match {

      case Some(1) =>
        (s"Helper.parseChar(gb)${if (since(typ).isDefined) " else '0'" else ""}",
         1)
      case Some(len) =>
        (s"Helper.parseString(gb,$len)${if (since(typ).isDefined) "else \"\""
        else ""}", len)
      case None => ("", 0) // variable length
    }

    if (since(typ).isDefined) {
      println("--handled")

    }

    (sb.append(str).toString(), len)
  }
}
trait AlwaysPresent
object SemanticType {
  def apply(s: Option[String]) =
    s.map(_ match {
      case "float"             => float
      case "int"               => int
      case "NumInGroup"        => NumInGroup
      case "String"            => String
      case "MultipleCharValue" => MultipleCharValue
      case "Exchange"          => Exchange
      case "MonthYear"         => MonthYear
      case "Qty"               => Qty
      case "Price"             => Price
      case "SeqNum"            => SeqNum
      case "char"              => char
      case "Currency"          => Currency
      case "LocalMktDate"      => LocalMktDate
      case "UTCTimestamp"      => UTCTimestamp
      case "Length"            => Length
    })
  case object enum extends SemanticType with OptValue {
    def primitive(typ: Type) = typ.primitiveType.get
    override def write(typ: Type, fieldName: String, bufferName: String) = {
      val et = Type.encodingTypes(typ.encodingType.get).complete()

      (if (typ.optional) {
        s"if($fieldName.isEmpty) ${putNone(et)} else Helper.put(b,$fieldName.get.to${et.encodingType.get})"
      } else s"Helper.put($bufferName,$fieldName.to${et.encodingType.get})", et.parseLength)
    }
    def parse(typ: Type): (String, Int) = {
      val sb = new StringBuffer
      sb append since(typ).getOrElse("")
      val et = Type.encodingTypes(typ.encodingType.get).complete()
      optValue(et) match {
        case "" =>
          sb append s"${typ.name}(Helper.parse${et.primitiveType.get}(gb))"
        case nv =>
          sb append s"Helper.parse${et.encodingType.get}(gb${optValue(et)}).map(d=> ${typ.name}(d))"
      }

      if (since(typ).isDefined) {
        sb append " else "
        if (typ.optional) sb append "None"
        else sb append s"${typ.name}(${constructor(typ, typ.nullValue)})"
      }

      (sb.toString(), 1)
    }

  }
  case object float extends SemanticType with NumericSemantics
  case object int   extends SemanticType with NumericSemantics
  case object NumInGroup
      extends SemanticType
      with StringReadSemantics
      with AlwaysPresent {
    override def parse(typ: Type): (String, Int) = {
      val sb                  = new StringBuilder
      val group               = Type.encodingTypes(typ.encodingType.get).complete()
      val (bl :: size :: Nil) = group.composition.map(_.complete)
      var len                 = 0

      {
        sb append "val blockLength = "
        val b = bl.semanticType.get.parse(bl)
        sb append b._1 + ".toInt" + eol
        len += b._2
      }
      {
        size.offset match {
          case Some(o) if o > len =>
            sb append s"Helper.skip(gb,${o - len})" + eol
            len = o
          case _ =>
        }
        sb append "var numInGroup = "
        val b = size.semanticType.get.parse(size)
        sb append b._1 + ".toInt"
        len += b._2
      }
      (sb.toString(), len)
    }
    override def write(typ: Type, fieldName: String, bufferName: String) = {
      val group                    = Type.encodingTypes(typ.encodingType.get).complete()
      val Array(list, blockLength) = fieldName.split(';')
      if (list == "OrderIDEntries")
        println("")
      val (bl :: size :: Nil) = group.composition.map(_.complete)
      var len                 = 0
      val sb                  = new StringBuilder

      {
        val b = bl.semanticType.get
          .write(bl, blockLength + s".to${bl.encodingType.get}", bufferName)
        sb append b._1 + eol
        len += b._2
      }
      {
        size.offset match {
          case Some(o) if o > len =>
            sb append s"Helper.fill($bufferName,${o - len})" + eol
            len = o
          case _ =>
        }
        val b = size.semanticType.get
          .write(bl, list + s".size.to${size.encodingType.get}", bufferName)
        sb append b._1
        len += b._2
      }
      (sb.toString, len)
    }
  }
  case object String extends SemanticType with StringSemantics
  case object MultipleCharValue
      extends SemanticType
      with StringSemantics
      with AlwaysPresent {
    override def write(typ: Type, fieldName: String, bufferName: String) =
      (s"Helper.put($bufferName,$fieldName.foldLeft(0){(bits,a) => bits+(1<<a.id)}.toByte)",
       1)

    override def parse(typ: Type): (String, Int) =
      (s"""{
        val byte = Helper.parseByte(gb)
        Helper.bitSet(byte, ${typ.name}.apply _) 
      }""",
       1)
  }
  case object Exchange extends SemanticType with StringSemantics {
    override def parse(typ: Type): (String, Int) = super.parse(typ)
  }
  case object MonthYear extends SemanticType with AlwaysPresent {
    def primitive(typ: Type): String = toString
    override def write(typ: Type, fieldName: String, bufferName: String) = {
      val sb = new StringBuilder
      val params = for (tr <- typ.composition; t = tr.complete()) {
        sb append t.semanticType.get
          .write(t, fieldName + "." + t.name, bufferName)
        sb append eol
      }
      sb append "//MonthYear"
      (sb.toString(), 5)
    }
    override def parse(typ: Type): (String, Int) = {
      val sb = new StringBuffer("{" + eol)
      val params = for (tr <- typ.composition; t = tr.complete()) yield {
        val varname = '_' + t.name
        sb append "val " + varname append " = "
        sb append t.semanticType.get.parse(t)._1 append eol
        varname
      }

      sb append params.mkString("MonthYear(", ", ", ")" + eol + "}" + eol)
      (sb.toString(), 5)
    }
  }
  case object Qty extends SemanticType with NumericSemantics
  case object Price extends SemanticType with NumericSemantics {
    override def parse(typ: Type): (String, Int) = super.parse(typ)
  }
  case object SeqNum   extends SemanticType with NumericSemantics
  case object char     extends SemanticType with StringSemantics
  case object Currency extends SemanticType with StringSemantics
  case object LocalMktDate extends SemanticType with NumericSemantics {
    override def write(typ: Type,
                       fieldName: String,
                       bufferName: String): (String, Int) =
      (if (typ.optional)
         s"if($fieldName.isEmpty) ${putNone(typ)} else Helper.put($bufferName,$fieldName.get)"
       else s"Helper.put(b,$fieldName)",
       typ.parseLength)
  }
  case object UTCTimestamp extends SemanticType with NumericSemantics
  case object Length       extends SemanticType with NumericSemantics

}
