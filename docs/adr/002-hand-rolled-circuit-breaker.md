# ADR-002: Hand-Rolled Circuit Breaker

**Context:** The rate-limit store wraps DynamoDB calls with a circuit breaker to prevent cascading failures during outages. We needed to decide between implementing our own or adopting a library.

## Decision

Implement a custom `CircuitBreaker[F]` using Cats Effect primitives (`Ref`, `Clock`, `Semaphore`) rather than adopting an off-the-shelf library like resilience4j-cats-effect or cats-retry.

## Why Hand-Rolled

- **Full control over state transitions.** The circuit breaker has three states (Closed → Open → HalfOpen → Closed|Open) with configurable failure thresholds, reset timeouts, and half-open probe counts. The state machine is small enough (about 260 lines) that owning it means no surprises when debugging its behaviour.
- **No extra dependency.** resilience4j brings a Java-centric API surface and transitive dependencies (Vavr, RxJava adapters). cats-retry is primarily a retry library — it can model some circuit breaker patterns but requires composition of multiple primitives. A purpose-built implementation keeps the dependency tree lean.
- **Cats Effect native.** Built directly on `Ref[F, State]` for lock-free state management and `Clock[F]` for time. No bridging between Java concurrency primitives and Cats Effect fibers.
- **Observable by construction.** After every call, whatever its outcome, the wrapper records the breaker's state as the CloudWatch gauge `CircuitBreakerState{CircuitBreaker=dynamodb-ratelimit}` (0 closed, 0.5 half-open, 1 open), which the `circuit-breaker-open` alarm watches, and as the Prometheus gauge `gate_circuit_breaker_state`. State changes are not published as Kinesis events. Wiring observability into a library's internal callbacks is always more fragile than owning the code.
- **Scoped by construction.** One breaker guards the rate-limit store for every key, so it must count only failures that mean DynamoDB is unhealthy: timeouts and transport or service faults. OCC conflicts, corrupt items, conditional-check failures, shed load (breaker open, bulkhead full), and config errors are not counted (`ResilientRateLimitStore.dependencyFailure`). Otherwise one contended key could open the breaker and, under `reject-all`, refuse every tenant. Owning the breaker makes this classification a plain function.

## What's Sacrificed

- **Edge cases a mature library has already met.** resilience4j has years of wide production use. Our implementation may have subtle bugs in timing edge cases (e.g., clock skew between fibers, race conditions in the HalfOpen → Closed transition under very high concurrency).
- **Community maintenance.** Bug fixes, performance improvements, and new features (e.g., slow-call detection, metrics integrations) come for free with a maintained library.
- **Testing burden.** We must write and maintain our own property-based tests and deterministic-time tests to cover the state machine. Without these, confidence in the implementation is lower than a library with an existing test suite.

## Mitigations

- Property-based tests (`CircuitBreakerPropertySpec`) verify the state-transition invariant: Closed → Open → HalfOpen → Closed|Open, with no direct Closed → HalfOpen transition.
- Deterministic time tests (`DeterministicTimeSpec`) use `TestControl` to verify exact open→halfOpen transitions without flaky sleeps.
- Fault injection tests (`ChaosSpec`) exercise the circuit breaker under intermittent failures, slow responses, and cascading failure scenarios.
- Scope tests (`CircuitBreakerScopeSpec`) check which failures may open the shared breaker.

## When to Reconsider

- If the circuit breaker develops bugs that are difficult to reproduce — a sign that the state machine is more complex than our test coverage reaches.
- If we need advanced features like slow-call-rate thresholds or per-exception-type failure counting, where a mature library has already solved the design.

## References

- `src/main/scala/resilience/CircuitBreaker.scala` — implementation
- `src/test/scala/resilience/CircuitBreakerPropertySpec.scala` — property-based tests
- `src/test/scala/resilience/DeterministicTimeSpec.scala` — TestControl tests
- `src/test/scala/resilience/ChaosSpec.scala` — fault injection tests
- `src/test/scala/resilience/CircuitBreakerScopeSpec.scala` — which failures count
- `src/main/scala/resilience/ResilientWrapper.scala` — the wrapper that applies it, records its state, and classifies failures

## Corrections

- 27 September 2026: this ADR said state transitions emit `CircuitBreakerStateChange` events to Kinesis. The event type was declared but never published, and was removed. The state is observable through the gauges above.
