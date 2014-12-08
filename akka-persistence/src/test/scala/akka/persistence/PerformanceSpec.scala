/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import scala.concurrent.duration._
import scala.language.postfixOps

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.testkit._

object PerformanceSpec {
  // multiply cycles with 200 for more
  // accurate throughput measurements
  val config =
    """
      akka.persistence.performance.cycles.warmup = 300
      akka.persistence.performance.cycles.load = 1000
    """

  case object StartMeasure
  case object StopMeasure
  final case class FailAt(sequenceNr: Long)

  trait Measure extends { this: Actor ⇒
    val NanoToSecond = 1000.0 * 1000 * 1000

    var startTime: Long = 0L
    var stopTime: Long = 0L

    var startSequenceNr = 0L
    var stopSequenceNr = 0L

    def startMeasure(): Unit = {
      startSequenceNr = lastSequenceNr
      startTime = System.nanoTime
    }

    def stopMeasure(): Unit = {
      stopSequenceNr = lastSequenceNr
      stopTime = System.nanoTime
      sender() ! (NanoToSecond * (stopSequenceNr - startSequenceNr) / (stopTime - startTime))
    }

    def lastSequenceNr: Long
  }

  abstract class PerformanceTestPersistentActor(name: String) extends NamedPersistentActor(name) with Measure {
    var failAt: Long = -1

    val controlBehavior: Receive = {
      case StartMeasure       ⇒ startMeasure()
      case StopMeasure        ⇒ stopMeasure()
      case FailAt(sequenceNr) ⇒ failAt = sequenceNr
    }

    override def postRestart(reason: Throwable) {
      super.postRestart(reason)
      receive(StartMeasure)
    }
  }

  class CommandsourcedTestPersistentActor(name: String) extends PerformanceTestPersistentActor(name) {

    override val controlBehavior: Receive = {
      case StartMeasure       ⇒ startMeasure()
      case StopMeasure        ⇒ defer(StopMeasure)(_ ⇒ stopMeasure())
      case FailAt(sequenceNr) ⇒ failAt = sequenceNr
    }

    val receiveRecover: Receive = {
      case _ ⇒ if (lastSequenceNr % 1000 == 0) print("r")
    }

    val receiveCommand: Receive = controlBehavior orElse {
      case cmd ⇒ persistAsync(cmd) { _ ⇒
        if (lastSequenceNr % 1000 == 0) print(".")
        if (lastSequenceNr == failAt) throw new TestException("boom")
      }
    }
  }

  class EventsourcedTestPersistentActor(name: String) extends PerformanceTestPersistentActor(name) {
    val receiveRecover: Receive = {
      case _ ⇒ if (lastSequenceNr % 1000 == 0) print("r")
    }

    val receiveCommand: Receive = controlBehavior orElse {
      case cmd ⇒ persist(cmd) { _ ⇒
        if (lastSequenceNr % 1000 == 0) print(".")
        if (lastSequenceNr == failAt) throw new TestException("boom")
      }
    }
  }

  class StashingEventsourcedTestPersistentActor(name: String) extends PerformanceTestPersistentActor(name) {
    val receiveRecover: Receive = {
      case _ ⇒ if (lastSequenceNr % 1000 == 0) print("r")
    }

    val printProgress: PartialFunction[Any, Any] = {
      case m ⇒ if (lastSequenceNr % 1000 == 0) print("."); m
    }

    val receiveCommand: Receive = printProgress andThen (controlBehavior orElse {
      case "a" ⇒ persist("a")(_ ⇒ context.become(processC))
      case "b" ⇒ persist("b")(_ ⇒ ())
    })

    val processC: Receive = printProgress andThen {
      case "c" ⇒
        persist("c")(_ ⇒ context.unbecome())
        unstashAll()
      case other ⇒ stash()
    }
  }
}

class PerformanceSpec extends AkkaSpec(PersistenceSpec.config("leveldb", "PerformanceSpec", serialization = "off").withFallback(ConfigFactory.parseString(PerformanceSpec.config))) with PersistenceSpec with ImplicitSender {
  import PerformanceSpec._

  val warmupCycles = system.settings.config.getInt("akka.persistence.performance.cycles.warmup")
  val loadCycles = system.settings.config.getInt("akka.persistence.performance.cycles.load")

  def stressCommandsourcedPersistentActor(failAt: Option[Long]): Unit = {
    val persistentActor = namedPersistentActor[CommandsourcedTestPersistentActor]
    failAt foreach { persistentActor ! FailAt(_) }
    1 to warmupCycles foreach { i ⇒ persistentActor ! s"msg${i}" }
    persistentActor ! StartMeasure
    1 to loadCycles foreach { i ⇒ persistentActor ! s"msg${i}" }
    persistentActor ! StopMeasure
    expectMsgPF(100 seconds) {
      case throughput: Double ⇒ println(f"\nthroughput = $throughput%.2f persistent actor commands per second")
    }
  }

  def stressPersistentActor(failAt: Option[Long]): Unit = {
    val persistentActor = namedPersistentActor[EventsourcedTestPersistentActor]
    failAt foreach { persistentActor ! FailAt(_) }
    1 to warmupCycles foreach { i ⇒ persistentActor ! s"msg${i}" }
    persistentActor ! StartMeasure
    1 to loadCycles foreach { i ⇒ persistentActor ! s"msg${i}" }
    persistentActor ! StopMeasure
    expectMsgPF(100 seconds) {
      case throughput: Double ⇒ println(f"\nthroughput = $throughput%.2f persistent events per second")
    }
  }

  def stressStashingPersistentActor(): Unit = {
    val persistentActor = namedPersistentActor[StashingEventsourcedTestPersistentActor]
    1 to warmupCycles foreach { i ⇒ persistentActor ! "b" }
    persistentActor ! StartMeasure
    val cmds = 1 to (loadCycles / 3) flatMap (_ ⇒ List("a", "b", "c"))
    persistentActor ! StartMeasure
    cmds foreach (persistentActor ! _)
    persistentActor ! StopMeasure
    expectMsgPF(100 seconds) {
      case throughput: Double ⇒ println(f"\nthroughput = $throughput%.2f persistent events per second")
    }
  }

  "A command sourced persistent actor" should {
    "have some reasonable throughput" in {
      stressCommandsourcedPersistentActor(None)
    }
    "have some reasonable throughput 2" in {
      stressCommandsourcedPersistentActor(None)
    }
  }

  "An event sourced persistent actor" should {
    "have some reasonable throughput" in {
      stressPersistentActor(None)
    }
    "have some reasonable throughput under failure conditions" in {
      stressPersistentActor(Some(warmupCycles + loadCycles / 10))
    }
    "have some reasonable throughput with stashing and unstashing every 3rd command" in {
      stressStashingPersistentActor()
    }
  }

}
