# Agent Testing Best Practices

Distilled from Google's Software Engineering book chapters on Testing Overview, Unit Testing, Test Doubles, and Larger Testing:

- https://abseil.io/resources/swe-book/html/ch11.html
- https://abseil.io/resources/swe-book/html/ch12.html
- https://abseil.io/resources/swe-book/html/ch13.html
- https://abseil.io/resources/swe-book/html/ch14.html

## Core Principle

A good test suite makes change safe. Tests are not just for catching bugs; they are executable documentation, design feedback, review support, and regression protection.

The suite only has value if engineers trust it. That trust comes from tests that are fast enough to run at the right time, deterministic enough to act on, clear enough to review, and realistic enough to catch the failures that matter.

Do not treat integration tests as an afterthought. Unit tests and integration tests answer different questions:

- Unit tests ask: does this behavior work under controlled conditions?
- Integration tests ask: do the real pieces work together correctly?
- End-to-end or large tests ask: does the important user or system workflow work with production-like wiring?

A strong agent-generated test plan uses all three deliberately. It should not be biased toward unit tests simply because they are easier to write. It should be biased toward confidence.

## Start With Risk, Not Test Type

Before writing tests, identify what could break and where confidence must come from. Choose the smallest test that still observes the real risk.

Use small unit tests when the risk is local behavior:

- Branching logic.
- Boundary values.
- Error handling.
- State transitions.
- Parsing or formatting.
- Permission decisions.
- Pure transformations.

Use integration tests when the risk is between components:

- API handler plus database.
- Service plus queue.
- Client plus server contract.
- ORM mappings and migrations.
- Serialization and deserialization.
- Auth middleware and routing.
- Cache behavior.
- File or object storage.
- Feature flag and configuration wiring.

Use large or end-to-end tests when the risk requires production-like fidelity:

- Browser behavior and real UI flows.
- Cross-service workflows.
- Deployment startup and configuration.
- Critical user journeys.
- Legacy systems that cannot be isolated safely.
- Smoke tests against staging or production-like environments.

The right test is not always the smallest possible test in absolute terms. It is the smallest test that would actually fail for the bug you care about.

## A Balanced Coverage Model

Comprehensive coverage does not mean high line coverage from one layer. It means important behavior is verified at the right layer.

Prefer this shape:

- Many focused unit tests for local behavior and edge cases.
- A healthy set of integration tests for real boundaries and infrastructure contracts.
- A smaller number of high-value end-to-end tests for critical workflows.

Avoid both extremes:

- Unit-test tunnel vision: many isolated tests with mocks, but little confidence that the application works when wired together.
- Ice cream cone testing: many slow end-to-end tests with weak unit and integration coverage.

Agents often overproduce unit tests because they are easy to generate. That is not enough. If the production change touches a database query, route, queue, external client, migration, serializer, UI flow, or configuration boundary, the agent should strongly consider integration coverage.

## Universal Test Qualities

### Test Behavior, Not Implementation

Write tests around observable behavior: given this state, when this action happens, then this result is visible. Avoid matching one test file or test method mechanically to each production method. A single method may contain multiple behaviors, and each meaningful behavior deserves focused coverage.

### Test Through Public or Realistic Boundaries

Exercise the code the way real callers use it. For unit tests, that usually means the public API of the module or class. For integration tests, that might mean HTTP requests, repository calls, message handlers, browser actions, CLI commands, or service APIs.

Tests that reach into private methods, internal state, or implementation-only collaborators become brittle when the implementation changes even though behavior remains correct.

### Verify Outcomes, Not Just Calls

Prefer state, output, or externally visible effects:

- Return value.
- API response.
- Database row.
- Emitted event.
- Queue message.
- File written.
- UI state.
- Permission denied.
- Audit log created.

Interaction verification is useful when the interaction itself is the behavior, such as sending an email, publishing an event, charging a payment method, or calling a third-party API. Otherwise, verifying that method `x()` called method `y()` usually exposes implementation details.

### Make Tests Clear Enough to Review

A reader should understand the test from the test body. Include important inputs, action, and expected output directly. Hide irrelevant setup behind helpers, but do not hide the core behavior being tested.

A strong test usually follows:

```text
Given: setup relevant state
When: perform one action
Then: verify observable result
```

### Name Tests After Behavior

Use names that explain the behavior and failure meaning:

```text
transferFunds_movesMoneyBetweenAccounts
registerUser_rejectsBannedUser
parseConfig_usesDefaultTimeoutWhenMissing
```

Avoid vague names:

```text
testTransferFunds
testRegister
testParse
```

### Keep Tests Hermetic Where Possible

A test should set up and tear down what it needs. It should not depend on test order, local machine state, shared databases, wall-clock time, network availability, random data, or leftover files from previous runs.

Hermeticity matters for all test sizes. It is easiest for unit tests, but integration and larger tests should still isolate data, use unique IDs, clean up resources, and avoid shared mutable fixtures where practical.

### Control Nondeterminism

Inject clocks, random generators, IDs, schedulers, and external services where possible. Avoid real sleeps.

Prefer:

- Fake clocks.
- Polling for an observable condition with a timeout.
- Explicit synchronization.
- Event-driven waits.
- Readiness checks.

Avoid:

```text
sleep(5)
```

The test should wait for the condition it needs, not for an arbitrary amount of time.

### Write Useful Failure Messages

Assertions should show actual and expected values clearly. Prefer expressive assertion libraries. A failure should tell the maintainer what behavior broke without requiring them to debug the test first.

### Prefer DAMP Over DRY in Tests

In test code, clarity is usually more important than removing every repetition. Some duplication is acceptable if it keeps each test readable. Over-abstracted helpers can make tests incomplete, obscure, and bug-prone.

## Unit Tests

Unit tests are the best tool for fast, precise feedback on local behavior. They should be easy to run frequently and easy to debug when they fail.

Use unit tests for:

- Pure logic.
- Edge cases.
- Error paths.
- State transitions.
- Validation.
- Policy decisions.
- Small API contracts.
- Regression tests for localized bugs.

Best practices:

- Keep the system under test small and explicit.
- Test one behavior per test.
- Use real lightweight collaborators when practical.
- Use fakes for expensive or nondeterministic dependencies.
- Avoid over-mocking internal collaborators.
- Keep setup minimal and visible.
- Make the expected result obvious.

Unit tests are necessary, but they are not sufficient when the bug can only appear through real wiring, data persistence, network boundaries, configuration, or runtime infrastructure.

## Integration Tests

Integration tests are first-class tests. They are not merely slower unit tests, and they are not optional polish. They validate the contracts between real components.

Use integration tests when correctness depends on two or more pieces working together:

- API route plus middleware plus handler.
- Handler plus database transaction.
- Repository plus real database schema.
- Service plus queue or event bus.
- Client plus server contract.
- Serializer plus real stored representation.
- Migration plus existing data shape.
- UI component plus router/data loader.
- CLI command plus filesystem.

Best practices:

### Define the Boundary

Be explicit about what is included and excluded:

```text
SUT: API server + real database
Real dependencies: Postgres, Redis
Fake dependencies: payment provider, email service
Out of scope: browser UI, production auth provider
```

### Use Real Dependencies Where They Matter

If the risk is SQL behavior, use a real database. If the risk is routing, send a real request through the router. If the risk is message shape, serialize and deserialize the real payload. Do not mock away the thing the integration test is supposed to prove.

### Prefer Ephemeral, Isolated Environments

Use test containers, temporary databases, in-memory services with faithful behavior, local service instances, temporary directories, or isolated schemas where appropriate. Each test should create its own data and avoid relying on preexisting global state.

### Keep Data Small and Purposeful

Seed only the records needed for the behavior under test. Large fixture worlds hide causality and make failures harder to diagnose.

### Verify Durable Outcomes

Check the real result at the boundary:

- HTTP status and response body.
- Database state after commit.
- Message published to the queue.
- File contents.
- Cache entry.
- Auth decision.
- Rendered UI state.

### Make Them Easy to Run

Integration tests should have a documented command and reasonable local workflow. If they require services, make setup explicit and repeatable.

## Large and End-to-End Tests

Large tests cover what unit and integration tests cannot: high-fidelity workflows, production-like configuration, and cross-system behavior.

Use them deliberately for:

- Critical user journeys.
- Browser-based workflows.
- Cross-service flows.
- Deployment or startup validation.
- Staging or production smoke checks.
- Compatibility across real clients and servers.
- Legacy areas where smaller tests are not yet feasible.

Best practices:

### Keep Them as Small as Fidelity Allows

Do not make one enormous end-to-end test for a whole product if several smaller integration or workflow tests would cover the same risk. A handful of targeted large tests is usually better than one broad, fragile scenario.

### Preserve the Thing You Need to Trust

Large tests exist for fidelity. Keep real the parts that create confidence:

- Real browser for browser confidence.
- Real service topology for deployment confidence.
- Real auth/config path for access-control confidence.
- Real migrations for upgrade confidence.

Use fakes only for parts outside the purpose of the test, such as payment providers, email delivery, or third-party APIs that would make the test unsafe or nondeterministic.

### Make Failures Diagnosable

Large tests are harder to debug, so they need better artifacts:

- Request IDs.
- Server logs.
- Screenshots or videos.
- Response bodies.
- Relevant configuration.
- Database snapshots or selected rows.
- Queue messages.
- Expected vs actual state.

### Assign Ownership

Every large test should have a clear owner responsible for failures, maintenance, triage, and documentation. Without ownership, large tests rot quickly because failures often span multiple systems.

### Run Them at the Right Time

Do not put every slow test in the inner development loop. A practical split is:

```text
Small unit tests: every edit / presubmit
Integration tests: presubmit or CI
Large end-to-end tests: release gate, nightly, or targeted CI
Production smoke tests: post-deploy monitoring
```

### Treat Flakiness as a Product Bug in the Test Suite

If a test is too flaky to trust, it should not be a release gate until fixed. Reruns can reduce short-term noise, but they do not replace root-cause fixes.

## Test Doubles

Test doubles are tools for controlling cost, speed, safety, and determinism. They should not be the default answer.

Prefer this order when practical:

1. Real implementation.
2. Faithful fake.
3. Stub or mock.

### Real Implementations

Use real dependencies when they are fast, deterministic, safe, and lightweight. Real implementations provide the best fidelity.

### Fakes

A good fake has real behavior and state, but is simpler or faster than the real dependency. Fakes are useful for databases, queues, storage clients, clocks, payment gateways, and remote APIs.

Ideally, the owner of the real API also owns the fake. If a fake is maintained separately, it should have contract tests or shared tests to keep it aligned with the real implementation.

### Stubs and Mocks

Use stubs and mocks when:

- The real dependency is unsafe, slow, expensive, or nondeterministic.
- The interaction itself is the behavior.
- You need to force a rare error path.
- A dependency cannot be run locally or hermetically.

Avoid mock-heavy tests that duplicate the production implementation. If a test requires mentally stepping through production code to understand why each stub exists, the test is probably too coupled to implementation details.

## Common Anti-Patterns

### Unit-Test Tunnel Vision

The agent writes many isolated tests but never verifies the real database, router, serializer, queue, UI, or configuration path touched by the change. This creates a false sense of coverage.

### Ice Cream Cone Testing

The suite relies heavily on slow end-to-end tests and has weak unit or integration coverage. Failures are expensive, slow to debug, and often flaky.

### Testing Private Implementation Details

Tests call private methods, inspect internal state, or verify internal collaborator calls. Refactors become painful even when behavior is unchanged.

### Mocking the Thing Being Validated

An integration test for persistence that mocks the database, or a routing test that bypasses the router, is not validating the actual risk.

### Overusing Mocks and Stubs

If the test mostly describes how dependencies should respond, it may be testing the mock setup more than the production behavior. Heavy stubbing also gets out of sync with real implementations.

### One Giant Test Per Method or Workflow

Large tests with many actions and assertions become hard to understand and hard to diagnose. Split by behavior or risk.

### Logic-Heavy Tests

Conditionals, loops, complex helpers, branching assertions, and generated expectations make tests harder to trust. Tests should be obvious on inspection.

### Hidden Important Setup

If a helper hides the exact input or expected behavior, the test becomes incomplete. Helpers should remove noise, not hide the point of the test.

### Shared Mutable Test Environments

Shared databases, shared users, shared queues, and shared staging state cause order dependence and cross-test interference.

### Real-Time Sleeps

Fixed sleeps make tests slow and flaky. Wait for observable readiness instead.

### Ignoring Flaky Tests

Flakiness destroys trust. Reruns can reduce short-term pain, but the root cause still needs to be fixed.

### Coverage Theater

Line coverage alone is not enough. A test that executes code without meaningful assertions does not prove behavior. Good coverage verifies important outcomes and failure modes.

## Agent Checklist

When an agent writes or reviews tests, it should ask:

1. What behavior changed from the user's or caller's point of view?
2. What can break locally, and what can break at component boundaries?
3. Which tests should be unit tests, which should be integration tests, and which need end-to-end fidelity?
4. Did the change touch persistence, routing, serialization, queues, auth, UI, configuration, deployment, or external contracts?
5. Are real dependencies used where their behavior matters?
6. Are fakes used where realism is needed but real dependencies are too costly or unsafe?
7. Are mocks limited to interactions that are truly part of the contract or hard-to-trigger error paths?
8. Does each test verify an observable outcome?
9. Is the test deterministic, isolated, and easy to debug?
10. Would this test still pass after a valid refactor?

The goal is not to maximize unit tests. The goal is to maximize trustworthy confidence with a maintainable mix of unit, integration, and large tests.
