package core

import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/** Two clients must never share a storage key (ADR-005), so the mapping has to
  * be injective, including for IDs built to collide.
  */
class TenantKeySpec
    extends AnyFreeSpec with ScalaCheckPropertyChecks with Matchers:

  // Heavy on ':' and digits, the characters that could forge a boundary.
  private val genPart: Gen[String] = Gen
    .listOf(Gen.oneOf(Gen.const(':'), Gen.const('1'), Gen.alphaNumChar))
    .map(_.mkString)

  "distinct (client, key) pairs never share a storage key" in
    forAll(genPart, genPart, genPart, genPart)((c1, k1, c2, k2) =>
      whenever((c1, k1) != (c2, k2))(
        TenantKey(c1, k1) should not be TenantKey(c2, k2),
      ),
    )

  "a colon cannot move the client boundary" in {
    TenantKey("a:b", "c") should not be TenantKey("a", "b:c")
    TenantKey("a", "1:b:c") should not be TenantKey("a:1:b", "c")
  }

  "a client ID that mimics the prefix cannot reach another client" in {
    // Client "3:abc" would write "t1:5:3:abc:..." and client "abc" writes
    // "t1:3:abc:...", so the length always disagrees.
    TenantKey("3:abc", "k") should not be TenantKey("abc", "k")
  }

  "names its version, so a new shape cannot read old rows" in {
    TenantKey("client", "key") shouldBe "t1:6:client:key"
  }
