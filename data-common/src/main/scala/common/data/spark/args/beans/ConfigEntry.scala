package common.data.spark.args.beans

abstract class ConfigEntry[T](
 val key: String,
 val doc:String,
 val parentConfig: List[ConfigEntry[_]],
 val childConfig: List[ConfigEntry[_]],
 val valueType: Class[_],
 val required: Boolean) {

  def defaultValueBoolean: Boolean
  def defaultValueString: String

  override def toString: String = {
    val s = s"Config Entry details :\n details: $doc \n value type: $valueType \n required if: $required \n default value: $defaultValueString" +
      s"\n child nodes details : ${childConfig.foreach(childConfig => childConfig.key + "::with details :"+ childConfig.doc)}"
    s
  }
}

class ConfigEntryWithStringDefault[T](
    key: String,
    doc:String,
    parentConfig: List[ConfigEntry[_]],
    childConfig: List[ConfigEntry[_]],
    valueType: Class[_],
    required: Boolean,
    default: String = "") extends ConfigEntry[T](
  key,
  doc,
  parentConfig,
  childConfig,
  valueType,
  required){
  override def defaultValueBoolean: Boolean = false

  override def defaultValueString: String = default
}

class ConfigEntryWithBooleanDefault[T](
  key: String,
  doc:String,
  parentConfig: List[ConfigEntry[_]],
  childConfig: List[ConfigEntry[_]],
  valueType: Class[_],
  required: Boolean,
  default: Boolean) extends ConfigEntry[T](
  key,
  doc,
  parentConfig,
  childConfig,
  valueType,
  required) {
  override def defaultValueBoolean: Boolean = default

  override def defaultValueString: String = ""
}
class ConfigEntryWithOptional[T](
  key: String,
  doc:String,
  parentConfig: List[ConfigEntry[_]],
  childConfig: List[ConfigEntry[_]],
  valueType: Class[_],
  required: Boolean) extends ConfigEntry[T](
  key,
  doc,
  parentConfig,
  childConfig,
  valueType,
  required) {
  override def defaultValueBoolean: Boolean = false

  override def defaultValueString: String = ""
}
