/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection. */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply
}

class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => pendingQueue :+= op
    case CopyFinished =>
      root ! PoisonPill
      root = newRoot
      pendingQueue.foreach(newRoot ! _)
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case op: Insert => doInsert(op)
    case op: Contains => doContains(op)
    case op: Remove => doRemove(op)
    case CopyTo(treeNode) => doCopyTo(treeNode)
  }

  private def doInsert(op: Insert) = {
    def insertIntoPos(pos: Position) = subtrees.get(pos) match {
      case Some(treeNode) => treeNode ! op
      case None =>
        val treeNode = context.actorOf(BinaryTreeNode.props(op.elem, initiallyRemoved = false))
        subtrees += pos -> treeNode
        op.requester ! OperationFinished(op.id)
    }

    if (op.elem == elem) {
      removed = false
      op.requester ! OperationFinished(op.id)
    } else if (op.elem < elem) insertIntoPos(Left) else insertIntoPos(Right)
  }

  private def doContains(op: Contains) = {
    def containsInPos(pos: Position) = subtrees.get(pos) match {
      case Some(treeNode) => treeNode ! op
      case None => op.requester ! ContainsResult(op.id, result = false)
    }

    if (op.elem == elem) {
      op.requester ! ContainsResult(op.id, !removed)
    } else if (op.elem < elem) containsInPos(Left) else containsInPos(Right)
  }

  private def doRemove(op: Remove) = {
    def removeFromPos(pos: Position) = subtrees.get(pos) match {
      case Some(treeNode) => treeNode ! op
      case None => op.requester ! OperationFinished(op.id)
    }

    if (op.elem == elem) {
      removed = true
      op.requester ! OperationFinished(op.id)
    } else if (op.elem < elem) removeFromPos(Left) else removeFromPos(Right)
  }

  private def doCopyTo(treeNode: ActorRef) = {
    val expected = subtrees.values.toSet
    if (removed && expected.isEmpty)
      context.parent ! CopyFinished
    else {
      if (!removed)
        treeNode ! Insert(self, 0, elem)
      expected.foreach(_ ! CopyTo(treeNode))
      context.become(copying(expected, removed))
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished =>
      val newExpected = expected - sender
      if (newExpected.isEmpty && insertConfirmed)
        context.parent ! CopyFinished
      else context.become(copying(newExpected, insertConfirmed))
    case OperationFinished(id) =>
      if (expected.isEmpty)
        context.parent ! CopyFinished
      else context.become(copying(expected, insertConfirmed = true))
  }
}
