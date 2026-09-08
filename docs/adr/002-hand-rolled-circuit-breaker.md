# ADR-002: Hand-Rolled Circuit Breaker

**Context:** The rate-limit store wraps DynamoDB calls with a circuit breaker to prevent cascading failures during outages. We needed to decide between implementing our own or adopting a library.

## Decision

Implement a custom `CircuitBreaker[F]` using Cats Effect primitives (`Ref`, `Clock`, `Semaphore`) rather than adopting an off-the-shelf library like resilience4j-cats-effect or cats-retry.

## Why Hand-Rolled

- **Full control over state transitions.** The circuit breaker has three states (Closed → Open → HalfOpen → Closed|Open) with configurable failure thresholds, reset timeouts, and half-open probe counts. The state machine is small enough (~200 lines) that owning it means no surprises when debugging production behaviour.
- **No extra dependency.** resilience4j brings a Java-centric API surface and transitive dependencies (Vavr, RxJava adapters). cats-retry is primarily a retry library — it can model some circuit breaker patterns but requires composition of multiple primitives. A purpose-built implementation keeps the dependency tree lean.
- **Cats Effect native.** Built directly on `Ref[F, State]` for lock-free state management and `Clock[F]` for time. No bridging between Java concurrency primitives and Cats Effect fibers.
- **Observable by construction.** State transitions emit `CircuitBreakerStateChange` events to Kinesis and update the `gate_circuit_breaker_state` Prometheus gauge. Wiring observability into a library's internal callbacks is always more fragile than owning the code.

## What's Sacrificed

- **Battle-tested edge cases.** resilience4j has years of production use across thousands of deployments. Our implementation may have subtle bugs in timing edge cases (e.g., clock skew between fibers, race conditions in the HalfOpen → Closed transition under very high concurrency).
- **Community maintenance.** Bug fixes, performance improvements, and new features (e.g., slow-call detection, metrics integrations) come for free with a maintained library.
- **Testing burden.** We must write and maintain our own property-based tests and deterministic-time tests to cover the state machine. Without these, confidence in the implementation is lower than a library with an existing test suite.

## Mitigations

- Property-based tests (`CircuitBreakerPropertySpec`) verify the state-transition invariant: Closed → Open → HalfOpen → Closed|Open, with no direct Closed → HalfOpen transition.
- Deterministic time tests (`DeterministicTimeSpec`) use `TestControl` to verify exact open→halfOpen transitions without flaky sleeps.
- Fault injection tests (`ChaosSpec`) exercise the circuit breaker under intermittent failures, slow responses, and cascading failure scenarios.

## When to Reconsider

- If the circuit breaker develops bugs that are difficult to reproduce — a sign that the state machine is more complex than our test coverage reaches.
- If we need advanced features like slow-call-rate thresholds or per-exception-type failure counting, where a mature library has already solved the design.

## References

- `src/main/scala/resilience/CircuitBreaker.scala` — implementation
- `src/test/scala/resilience/CircuitBreakerPropertySpec.scala` — property-based tests
- `src/test/scala/resilience/DeterministicTimeSpec.scala` — TestControl tests
- `src/test/scala/resilience/ChaosSpec.scala` — fault injection tests
