package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.concurrent.duration._
import scala.Some
import akka.event.LoggingReceive
import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  /**
   * Instructs the primary to insert the (key, value) pair into the storage and replicate it to the secondaries:
   * id is a client-chosen unique identifier for this request.
   */
  case class Insert(key: String, value: String, id: Long) extends Operation
  /**
   * Instructs the primary to remove the key (and its corresponding value) from the storage
   * and then remove it from the secondaries.
   */
  case class Remove(key: String, id: Long) extends Operation
  /**
   * Instructs the replica to look up the "current" value assigned with the key in the storage and reply with
   * the stored value.
   */
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  /**
   * A successful Insert or Remove results in a reply to the client in the form of an OperationAck(id) message
   * where the id field matches the corresponding id field of the operation that has been acknowledged.
   */
  case class OperationAck(id: Long) extends OperationReply
  /**
   * A failed Insert or Remove command results in an OperationFailed(id) reply.
   * A failure is defined as the inability to confirm the operation within 1 second.
   */
  case class OperationFailed(id: Long) extends OperationReply
  /**
   * A Get operation results in a GetResult(key, valueOption, id) message where the id field matches the value
   * in the id field of the corresponding Get message. The valueOption field should contain None if the key is not present
   * in the replica or Some(value) if a value is currently assigned to the given key in that replica.
   */
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val persistence = context.actorOf(persistenceProps, "persistence")

  var snapshotSeq = 0
  var persistAcks = Map.empty[Long, ActorRef]
  var replicateAcks = Map.empty[Long, (ActorRef, Set[ActorRef])]

  var persistRepeaters = Map.empty[Long, Cancellable]
  var replicationFailureReporters = Map.empty[Long, Cancellable]

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5, loggingEnabled = true) {
    case _: PersistenceException => SupervisorStrategy.Restart
  }

  override def preStart() {
    arbiter ! Join
    context.watch(persistence)
  }

  def receive = LoggingReceive {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {
    case Get(key, id) =>
      val valueOption = kv.get(key)
      sender ! GetResult(key, valueOption, id)

    // Whenever the primary replica receives an update operation (either Insert or Remove)
    // it must reply with an OperationAck(id) or OperationFailed(id) message, to be sent at most 1 second after
    // the update command was processed (the ActorSystem’s timer resolution is deemed to be sufficiently precise for this).
    //
    // A positive OperationAck reply must be sent as soon as
    // - the change in question has been handed down to the Persistence module
    //   and a corresponding acknowledgement has been received from it
    // - replication of the change in question has been initiated
    //   and all of the secondary replicas have acknowledged the replication of the update.

    case Insert(key, value, id) =>
      kv += key -> value
      initiateReplication(key, Some(value), id)

    case Remove(key, id) =>
      kv -= key
      initiateReplication(key, None, id)

    case Persisted(key, id) =>
      persistRepeaters(id).cancel()
      persistRepeaters -= id

      val origSender = persistAcks(id)
      persistAcks -= id

      if(!replicateAcks.contains(id)) {
        replicationFailureReporters(id).cancel()
        replicationFailureReporters -= id
        origSender ! OperationAck(id)
      }

    case Replicated(key, id) =>
      if(replicateAcks.contains(id)) {
        val (origSender, currAckSet) = replicateAcks(id)
        val newAckSet = currAckSet - sender

        if (newAckSet.isEmpty) replicateAcks -= id
        else replicateAcks = replicateAcks.updated(id, (origSender, newAckSet))

        if(!replicateAcks.contains(id) && !persistAcks.contains(id)) {
          replicationFailureReporters(id).cancel()
          replicationFailureReporters -= id
          origSender ! OperationAck(id)
        }
      }

    // If replicas leave the cluster, which is signalled by sending a new Replicas message to the primary,
    // then outstanding acknowledgements of these replicas must be waived.
    // This can lead to the generation of an OperationAck triggered indirectly by the Replicas message.

    case Replicas(replicas) =>
      val secondaryReplicas = replicas.filterNot(_ == self)
      val removed = secondaries.keySet -- secondaryReplicas
      val added = secondaryReplicas -- secondaries.keySet

      var addedSecondaries = Map.empty[ActorRef, ActorRef]
      val addedReplicators = added map { replica =>
        val replicator = context.actorOf(Replicator.props(replica))
        addedSecondaries += replica -> replicator
        replicator
      }

      removed foreach { replica =>
        secondaries(replica) ! PoisonPill
      }
      removed foreach { replica =>
        replicateAcks.foreach { case (id, (origSender, rs)) =>
          if (rs.contains(secondaries(replica))) self.tell(Replicated("", id), secondaries(replica))
        }
      }

      replicators = replicators -- removed.map(secondaries) ++ addedReplicators
      secondaries = secondaries -- removed ++ addedSecondaries

      addedReplicators foreach { replicator =>
        kv.zipWithIndex foreach { case ((key, value), idx) =>
          replicator ! Replicate(key, Some(value), idx)
        }
      }
  }

  private def initiateReplication(key: String, valueOption: Option[String], id: Long) = {
    persistAcks += id -> sender

    if (replicators.nonEmpty) {
      replicateAcks += id -> (sender, replicators)
      // replicate Insert or Remove commands to the secondaries
      replicators foreach {
        _ ! Replicate(key, valueOption, id)
      }
    }

    persistRepeaters += id -> context.system.scheduler.schedule(
      0 millis, 100 millis, persistence, Persist(key, valueOption, id)
    )

    // A failed Insert or Remove command results in an OperationFailed(id) reply.
    // A failure is defined as the inability to confirm the operation within 1 second.
    replicationFailureReporters += id -> context.system.scheduler.scheduleOnce(1 second) {
      reportReplicationFailure(id)
    }
  }

  private def reportReplicationFailure(id: Long) = {
      if(persistRepeaters.contains(id)) {
        persistRepeaters(id).cancel()
        persistRepeaters -= id
      }

      replicationFailureReporters -= id

      val origSender =
        if(persistAcks.contains(id)) persistAcks(id)
        else replicateAcks(id)._1

      persistAcks -= id
      replicateAcks -= id
      origSender ! OperationFailed(id)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) =>
      if(seq < snapshotSeq) sender ! SnapshotAck(key, seq)

      if (seq == snapshotSeq) {
        valueOption match {
          case None => kv -= key
          case Some(value) => kv += key -> value
        }

        snapshotSeq += 1
        persistAcks += seq -> sender

        persistRepeaters += seq -> context.system.scheduler.schedule(
          0 millis, 100 millis, persistence, Persist(key, valueOption, seq)
        )
      }

    case Persisted(key, id) =>
      val sender = persistAcks(id)
      persistAcks -= id
      persistRepeaters(id).cancel()
      persistRepeaters -= id
      sender ! SnapshotAck(key, id)
  }
}
