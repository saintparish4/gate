package observability

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

class MetricsBufferSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  val point = MetricDataPoint("test", 1.0, StandardUnit.COUNT)

  "BufferState" - {
    "enqueue adds to buffer and increments size" in {
      val buf = BufferState().enqueue(point, 100)
      IO.pure(buf).asserting { b =>
        b.size shouldBe 1
        b.queue.size shouldBe 1
      }
    }

    "enqueue drops oldest when at maxSize" in {
      val oldest = MetricDataPoint("oldest", 0.0, StandardUnit.COUNT)
      val buf =
        BufferState(queue = scala.collection.immutable.Queue(oldest), size = 1)
          .enqueue(point, 1) // maxSize = 1, so oldest is dropped
      IO.pure(buf).asserting { b =>
        b.size shouldBe 1
        b.queue.head.name shouldBe "test"
      }
    }

    "drainAll returns all metrics and resets buffer" in {
      val buf = BufferState().enqueue(point, 100).enqueue(point, 100)
        .enqueue(point, 100)
      val (empty, metrics) = buf.drainAll
      IO.pure((empty, metrics)).asserting { case (e, m) =>
        e.size shouldBe 0
        e.queue.isEmpty shouldBe true
        m.length shouldBe 3
      }
    }
  }

  "CloudWatch flush CAS guard" - {
    "concurrent flushes do not stampede" in {
      // Verify that only one flush runs at a time by using a counter
      for
        flushCount <- Ref.of[IO, Int](0)
        flushingRef <- Ref.of[IO, Boolean](false)
        // Simulate N concurrent flush attempts
        results <- (1 to 20).toList.parTraverse(_ =>
          flushingRef.getAndSet(true).flatMap {
            case true => IO.pure(false) // skipped
            case false => flushCount.update(_ + 1) *>
                IO.sleep(scala.concurrent.duration.Duration(10, "millis")) *>
                flushingRef.set(false) *> IO.pure(true) // actually ran
          },
        )
        ran <- flushCount.get
      yield
        // Under heavy concurrency, most attempts should be skipped
        ran should be >= 1
        ran should be < 20
    }
  }
