package common.data.spark.args.beans


object ConfigHelper{

  def toBoolean(a: Any): Boolean = a match {
    case a :String => a.trim.toBoolean
    case a :Boolean => a
    case _ => throw new IllegalArgumentException(s"Invalid value passed for boolean $a")
  }
}

class TypeConfiguration[T](
  val parent: ConfigBuilder ){
  import ConfigHelper._

  def this(parent: ConfigBuilder) = {
    this(parent)
  }

  def createWithDefaultString: ConfigEntry[T] ={
    val entry = new ConfigEntryWithStringDefault[T](
      parent.key, parent._doc,parent._parentConfig,parent._childConfig,parent._valueType,parent._required,parent._default.toString
    )
    entry
  }
  def createWithDefaultBoolean: ConfigEntry[T]={
    val entry = new ConfigEntryWithBooleanDefault[T](
      parent.key, parent._doc,parent._parentConfig,parent._childConfig,parent._valueType,parent._required,toBoolean(parent._default)
    )
    entry
  }
  def createWithOptional: ConfigEntry[T]={
    val entry = new ConfigEntryWithStringDefault[T](
      parent.key, parent._doc,parent._parentConfig,parent._childConfig,parent._valueType,false,""
    )
    entry
  }
}




/**
 * This is a helper class to define configurations pattern for the
 * execution. This can be used while assigning values to the configurations
 * and also to define [[_childConfig]] on [[_parentConfig]] to be used for validations
 *
 */
case class ConfigBuilder(key: String){

  private[spark] var _doc  = ""
  private[spark] var _parentConfig: List[ConfigEntry[_]] = List.empty[ConfigEntry[_]]
  private[spark] var _childConfig: List[ConfigEntry[_]] = List.empty[ConfigEntry[_]]
  private[spark] var _valueType: Class[_] = classOf[String]
  private[spark] var _required: Boolean = false
  private[spark] var _default: Any = null


  def doc(s: String): ConfigBuilder = {
    _doc = s
    this
  }

  /**
   *  Add the defined parent KEYS to this config class.
   * @param v
   * @return
   */
  def withParent(v: ConfigEntry[_]): ConfigBuilder = {
    _parentConfig = _parentConfig :+ v
    this
  }

  /**
   *  Add the defined child KEYS to this config class.
   * @param v
   * @return
   */
  def withChildren(v: ConfigEntry[_]): ConfigBuilder = {
    _childConfig = _childConfig :+ v
    this
  }
  def withValueType(c: Class[_]): ConfigBuilder = {
    _valueType = c
    this
  }
  def setRequired(b: Boolean): ConfigBuilder ={
    _required = b
    this
  }
  def withDefault(s: Any): ConfigBuilder ={
    _default = s
    this
  }

  def stringConfig: TypeConfiguration[String]= {
      new TypeConfiguration(parent = this)
  }
  def booleanConfig: TypeConfiguration[Boolean] = {
      new TypeConfiguration(parent = this)
  }
}
