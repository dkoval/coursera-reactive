package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8

    // to complete: additional parameters of simulation
    val incubationTime = 6
    val dieTime = 14
    val immuneTime = 16
    val recoveryTime = 18
    val prevalenceRate = 0.01
    val transRate = 0.4
    val dieRate = 0.25
  }

  import SimConfig._

  // to complete: construct list of persons
  val persons: List[Person] = for (i <- (0 until population).toList) yield {
    val p = new Person(i)
    // TODO: think of a better strategy
    if (i < population * prevalenceRate)
      p.setInfected()
    p.scheduleNextMove()
    p
  }

  class Person (val id: Int) {
    var infected = false
    var sick = false
    var immune = false
    var dead = false

    // demonstrates random number generation
    var row: Int = randomBelow(roomRows)
    var col: Int = randomBelow(roomColumns)

    //
    // to complete with simulation logic
    //
    def setInfected() {
      infected = true

      // Rule 4. When a person becomes infected, he does not immediately get sick,
      // but enters a phase of incubation in which he is infectious but not sick.
      // Rule 5. After 6 days of becoming infected, a person becomes sick and is therefore visibly infectious.
      afterDelay(incubationTime) {
        sick = true
      }

      // Rule 6. After 14 days of becoming infected, a person dies with a probability of 25%.
      // Dead people do not move, but stay visibly infectious.
      afterDelay(dieTime) {
        // TODO: think of a better strategy
        if (random < dieRate)
          dead = true
      }

      // Rule 7. After 16 days of becoming infected, a person becomes immune.
      // He is no longer visibly infectious, but remains infectious.
      // An immune person cannot get infected.
      afterDelay(immuneTime) {
        if (!dead) {
          immune = true
          sick = false
        }
      }

      // Rule 8. After 18 days of becoming infected, a person turns healthy.
      // He is now in the same state as he was before his infection, which means that he can get infected again.
      afterDelay(recoveryTime) {
        if (!dead) {
          immune = false
          infected = false
        }
      }
    }

    def scheduleNextMove() {
      // Rule 1. A person moves to one of their neighbouring rooms (left, right, up, down)
      // within the next 5 days (with equally distributed probability).
      val moveDelay = randomBelow(5) + 1
      afterDelay(moveDelay)(move())
    }

    private def move() {
      if (dead) {
        return
      }

      // Rule 3. When a person moves into a room with an infectious person he might get infected
      // according to the transmissibility rate, unless the person is already infected or immune.
      if (!(infected || immune)) {
        val curRoom: (Int, Int) = (row, col)
        if ((random < transRate) && isInfectedRoom(curRoom))
          setInfected()
      }

      val upRow = (row - 1 + roomRows) % roomRows
      val downRow = (row + 1) % roomRows
      val leftCol = (col - 1 + roomColumns) % roomColumns
      val rightCol = (col + 1) % roomColumns

      // neighbour rooms
      val neighborRooms = List(
        (upRow, col),
        (downRow, col),
        (row, leftCol),
        (row, rightCol)
      )

      // Rule 2. A person avoids rooms with sick or dead (visibly infectious) people.
      // This means that if a person is surrounded by visibly infectious people, he does not change position;
      // however, he might change position the next time he tries to move
      // (for example, if a visibly infectious person moved out of one of the neighbouring rooms or became immune).
      val healthyNeighborRooms = neighborRooms.filter(isHealthyRoom)
      if (!healthyNeighborRooms.isEmpty) {
        val nextRoom = healthyNeighborRooms(randomBelow(healthyNeighborRooms.length))
        row = nextRoom._1
        col = nextRoom._2
      }

      scheduleNextMove()
    }

    private def isPersonInRoom(p: Person, room: (Int, Int)): Boolean = {
      (p.row == room._1) && (p.col == room._2)
    }

    private def isHealthyRoom(room: (Int, Int)): Boolean = {
      // Make sure nobody in the room (row, col) is either sick or dead
      persons.count(p => isPersonInRoom(p, room) && (p.sick || p.dead)) == 0
    }

    private def isInfectedRoom(room: (Int, Int)): Boolean = {
      // The room (row, col) contains at least one infected person
      persons.count(p => isPersonInRoom(p, room) && p.infected) != 0
    }
  }
}
