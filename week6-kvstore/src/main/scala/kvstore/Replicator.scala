package kvstore

import akka.actor.{Cancellable, Props, Actor, ActorRef}
import scala.concurrent.duration._
import akka.event.LoggingReceive
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props[Replicator](new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var repeaters = Map.empty[Long, Cancellable]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = LoggingReceive {
    case replicate @ Replicate(key, valueOption, id) => {
      val seq = nextSeq
      acks += seq -> (sender, replicate)

      val snapshotRepeater = context.system.scheduler.schedule(
        0 millis, 250 millis, replica, Snapshot(key, valueOption, seq))

      repeaters += seq -> snapshotRepeater
    }

    case SnapshotAck(key, seq) => {
      if(acks.contains(seq)) {
        val (origSender, Replicate(key, _, id)) = acks(seq)
        acks -= seq
        repeaters(seq).cancel()
        repeaters -= seq
        origSender ! Replicated(key, id)
      }
    }
  }

}