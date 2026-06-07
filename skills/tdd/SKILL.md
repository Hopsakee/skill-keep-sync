---
name: tdd
description: "Vertical-slice test-driven development. ONE test â ONE implementation â repeat. Hard rule against horizontal slicing (writing all tests first, then all impl). Tests verify behavior through public interfaces, not implementation details. USE WHEN tdd, red green refactor, test driven, test-first, write tests, tracer bullet, vertical slice tdd."
title: tdd
active_version: 1
---

# Test-Driven Development

Vertical-slice TDD: each cycle is one test â one piece of implementation â repeat. The discipline is **anti-horizontal-slicing** â never write all tests first and then all implementation.

## Philosophy

**Core principle**: Tests verify behavior through public interfaces, not implementation details. Code can change entirely; tests shouldn't.

**Good tests** are integration-style. They exercise real code paths through public APIs. They describe *what* the system does, not *how* it does it. A good test reads like a specification â `"catalog ingests a document without an id"` tells you exactly what capability exists. These tests survive refactors because they don't care about internal structure.

**Bad tests** are coupled to implementation. They mock internal collaborators, test private methods, or verify through external means (querying a DB directly instead of using the interface). The warning sign: your test breaks when you refactor, but behavior hasn't changed.

See [references/tests.md](references/tests.md) for examples and [references/mocking.md](references/mocking.md) for mocking guidelines.

## Anti-Pattern: Horizontal Slices

**DO NOT write all tests first, then all implementation.**

This is horizontal slicing â treating RED as "write all tests" and GREEN as "write all code." It produces crap tests:

- Tests written in bulk test *imagined* behavior, not *actual* behavior
- You end up testing the *shape* of things (data structures, signatures) rather than user-facing behavior
- Tests become insensitive to real changes â pass when behavior breaks, fail when behavior is fine
- You outrun your headlights, committing to test structure before understanding the implementation

```
WRONG (horizontal):
  RED:   test1, test2, test3, test4, test5
  GREEN: impl1, impl2, impl3, impl4, impl5

RIGHT (vertical):
  REDâGREEN: test1âimpl1
  REDâGREEN: test2âimpl2
  REDâGREEN: test3âimpl3
  ...
```

Each cycle informs the next. Because you just wrote the code, you know exactly what behavior matters and how to verify it.

## Workflow

### 1. Planning (do once at the start)

Before writing any code:

- [ ] Confirm with the user which interface changes are needed
- [ ] Confirm which behaviors to test (you can't test everything â pick the critical paths)
- [ ] Identify opportunities for [deep modules](references/deep-modules.md) â small interface, deep implementation
- [ ] List the behaviors to test (not implementation steps)
- [ ] Get the user's approval on the plan

Ask: *"What should the public interface look like? Which behaviors matter most to test?"*

**Critical**: Confirm exactly which behaviors are in scope. A good rule of thumb â don't solve things that might never be a problem. Focus testing effort on critical paths and complex logic, not every conceivable edge case.

### 2. Tracer Bullet (first cycle)

Write ONE test that confirms ONE thing about the system end-to-end:

```
RED:   Write test for first behavior â test fails (ideally with a clear error)
GREEN: Write minimal code to pass â test passes
```

The tracer bullet proves the path works end-to-end before you invest in any of it.

### 3. Incremental Loop (each subsequent behavior)

```
RED:   Write next test â fails
GREEN: Minimal code to pass â passes
```

Rules:

- **One test at a time**
- **Only enough code to pass the current test** â no speculative implementation
- **Don't anticipate future tests** â let them drive the next cycle
- **Keep tests focused on observable behavior**

### 4. Refactor (only when GREEN)

After all tests pass, look for refactor candidates:

- Extract duplication
- Deepen modules â move complexity behind simple interfaces
- Apply SOLID where it's natural, not dogmatic
- Consider what the new code reveals about existing code
- Run tests after each refactor step

**Never refactor while RED.** Get to GREEN first.

## Per-cycle checklist

```
[ ] Test describes behavior, not implementation
[ ] Test uses public interface only
[ ] Test would survive an internal refactor
[ ] Code is minimal for this test
[ ] No speculative features added
```

## Stack notes (Python)

- **pytest** is the default. Use fixtures for shared setup, parametrize for multiple inputs of the same behavior.
- Avoid `unittest.mock.patch` on your own internal modules â that couples tests to implementation. See [references/mocking.md](references/mocking.md).
- **UI behavior** (web frameworks, GUIs): test the data layer and pure functions with unit tests; verify rendered UI by opening the page / screenshotting rather than fighting brittle headless-browser assertions.
- **Async code**: use `pytest-asyncio`. Don't mix sync and async in the same test.

## Constraints

- **Don't add error handling, fallbacks, or validation for scenarios the tests don't drive.** Trust internal code; validate at system boundaries only.
- **Don't write a test for a behavior the user hasn't asked for.** That's speculation, not TDD.
- **Don't refactor while RED.** First make it green, then make it clean.
- **Don't mock your own modules.** See [references/mocking.md](references/mocking.md) for boundaries.
