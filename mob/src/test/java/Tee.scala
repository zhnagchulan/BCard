import com.wanda.mob.spark.streaming.event.BaseEvent

import scala.beans.BeanProperty

case class Tee()extends BaseEvent{

  @BeanProperty var AID : String = _
  @BeanProperty var BBB : String = _
  @BeanProperty var CCC : String = _
  @BeanProperty var OK : String = _
  @BeanProperty var NO : String = _

}
