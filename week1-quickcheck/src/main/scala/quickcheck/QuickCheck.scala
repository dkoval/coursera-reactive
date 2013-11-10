package quickcheck

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._
import Math._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  // If you insert any two elements into an empty heap,
  // finding the minimum of the resulting heap
  // should get the smallest of the two elements back.
  property("hint1") = forAll { (a: A, b: A) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == min(a, b)
  }

  // If you insert an element into an empty heap, then delete the minimum,
  // the resulting heap should be empty.
  property("hint2") = forAll { a: A =>
    val h = deleteMin(insert(a, empty))
    isEmpty(h)
  }

  // Given any heap, you should get a sorted sequence of elements
  // when continually finding and deleting minima.
  // (Hint: recursion and helper functions are your friends.)
  property ("hint3") = forAll { h: H =>
    def isSortedHeap(h: H) : Boolean =
      if (isEmpty(h)) true
      else {
        val minEl = findMin(h)
        val remHeap = deleteMin(h)
        // make sure that after deletion of the min element from heap h
        // the minEl value is still less than or equal to the minimum element
        // of the resulting heap remHeap;
        // additionally, the remHeap heap should remain sorted.
        // NOTE: isEmpty(remHeap) check is not obsolete here and aims to handle
        // java.util.NoSuchElementException: min of empty heap
        // error (see BinomialHeap#findMin() method)
        isEmpty(remHeap) || ((minEl <= findMin(remHeap)) && isSortedHeap(remHeap))
      }

    isSortedHeap(h)
  }

  // Finding a minimum of the melding of any two heaps
  // should return a minimum of one or the other.
  property ("hint4") = forAll { (h1: H, h2: H) =>
    val meldedHeap = meld(h1, h2)
    findMin(meldedHeap) == min(findMin(h1), findMin(h2))
  }

  property("extra1") = forAll { (h1: H, h2: H) =>
    def areEqualHeaps(h1: H, h2: H): Boolean =
      if (isEmpty(h1) && isEmpty(h2)) true
      else {
        val minEl1 = findMin(h1)
        val minEl2 = findMin(h2)
        // assert the two heaps are still equal after deletion of
        // the minimum elements
        (minEl1 == minEl2) && areEqualHeaps(deleteMin(h1), deleteMin(h2))
      }

    // assert that h1 + h2 is the same as: (h1 - min_h1) + (min_h1 + h2)
    areEqualHeaps(meld(h1, h2), meld(deleteMin(h1), insert(findMin(h1), h2)))
  }

  lazy val genHeap: Gen[H] = for {
    n <- arbitrary[A]
    h <- oneOf(value(empty), genHeap)
  } yield insert(n, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
