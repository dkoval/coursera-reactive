package simulations

class Wire {
  private var sigVal = false
  private var actions: List[Simulator#Action] = List()

  def getSignal: Boolean = sigVal
  
  def setSignal(s: Boolean) {
    if (s != sigVal) {
      sigVal = s
      actions.foreach(action => action())
    }
  }

  def addAction(a: Simulator#Action) {
    actions = a :: actions
    a()
  }
}

abstract class CircuitSimulator extends Simulator {

  val InverterDelay: Int
  val AndGateDelay: Int
  val OrGateDelay: Int

  def probe(name: String, wire: Wire) {
    wire addAction {
      () => afterDelay(0) {
        println(
          "  " + currentTime + ": " + name + " -> " +  wire.getSignal)
      }
    }
  }

  def inverter(input: Wire, output: Wire) {
    def invertAction() {
      val inputSig = input.getSignal
      afterDelay(InverterDelay) { output.setSignal(!inputSig) }
    }
    input addAction invertAction
  }

  def andGate(a1: Wire, a2: Wire, output: Wire) {
    def andAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(AndGateDelay) { output.setSignal(a1Sig & a2Sig) }
    }
    a1 addAction andAction
    a2 addAction andAction
  }

  //
  // to complete with orGates and demux...
  //

  def orGate(a1: Wire, a2: Wire, output: Wire) {
    def orAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(OrGateDelay) { output.setSignal(a1Sig | a2Sig) }
    }
    a1 addAction orAction
    a2 addAction orAction
  }
  
  def orGate2(a1: Wire, a2: Wire, output: Wire) {
    val notA1, notA2, notOut = new Wire()
    // According to De Morgan's law,
    // (a1 | a2) = not(not(a1) & not(a2))
    // http://en.wikipedia.org/wiki/De_Morgan's_laws
    inverter(a1, notA1); inverter(a2, notA2)
    andGate(notA2, notA2, notOut)
    inverter(notOut, output)
  }

  def demux(in: Wire, c: List[Wire], out: List[Wire]) {
    c match {
      // The simplest demultiplexer has 0 control wires and 1 output wire.
      case Nil => andGate(in, in, out(0))
      // The demultiplexer directs the signal from an input wire based on the control signal.
      // The rest of the output signals are set to 0.
      // https://class.coursera.org/reactive-001/assignment/view?assignment_id=7
      case first :: rest => {
        // Split the input into 2 parts: inLeft = (in & first), nRight = (in & not(first)),
        // then recursively apply the demux() function to the left part of the input and
        // the remaining control signals. This will form the first n elements of the output.
        // Analogously, recursively call the demux() function on the right part of the output
        // with the same set of control signals to compute the rest (n + 1 ... N - 1) elements
        // of the output.
        val inLeft, inRight, notFirst = new Wire
        // inLeft = (in & first)
        andGate(in, first, inLeft)
        // inRight = (in & not(first))
        inverter(first, notFirst); andGate(in, notFirst, inRight)

        val n = out.length / 2
        demux(inLeft, rest, out.take(n))
        demux(inRight, rest, out.drop(n))
      }
    }
  }
}

object Circuit extends CircuitSimulator {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5

  def andGateExample {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    probe("in1", in1)
    probe("in2", in2)
    probe("out", out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    in1.setSignal(true)
    run

    in2.setSignal(true)
    run
  }

  //
  // to complete with orGateExample and demuxExample...
  //
}

object CircuitMain extends App {
  // You can write tests either here, or better in the test class CircuitSuite.
  Circuit.andGateExample
}
