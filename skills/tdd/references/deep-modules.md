# Deep Modules

From John Ousterhout's *A Philosophy of Software Design*.

**Deep module** = small interface + lots of implementation

```
┌─────────────────────┐
│   Small Interface   │  ← Few methods, simple params
├─────────────────────┤
│                     │
│                     │
│  Deep Implementation│  ← Complex logic hidden
│                     │
│                     │
└─────────────────────┘
```

**Shallow module** = large interface + little implementation (avoid)

```
┌─────────────────────────────────┐
│       Large Interface           │  ← Many methods, complex params
├─────────────────────────────────┤
│  Thin Implementation            │  ← Just passes through
└─────────────────────────────────┘
```

## Why deep modules matter for TDD

- **Tests against a small interface stay stable.** Refactoring the deep implementation doesn't break tests.
- **Tests against a large interface explode.** Every internal change ripples through the test suite.
- **Shallow modules force tests to be implementation-coupled** because there *is* nothing else to test against.

If your tests are fragile, the module is probably too shallow. The fix is usually to push complexity *into* the module, not pull it out.

## Questions to ask while designing

- Can I reduce the number of methods?
- Can I simplify the parameters? (Replace many primitives with one well-named object?)
- Can I hide more complexity inside?
- Does the caller need to know about this implementation detail, or is it leaking?
- If I deleted this method, would callers actually miss it — or would I just push the same logic up one level?

## The "two adapters" rule

> One adapter = a hypothetical seam. Two adapters = a real seam.

A deep module exposing a single adapter is not yet a real abstraction — you're guessing. Wait until a second adapter actually shows up before extracting the seam. This is the same idea as "don't solve things that might never be a problem."

## Concrete example

**Shallow** (hypothetical document catalog):

```python
class Catalog:
    def normalize_document(self, d): ...
    def deduplicate(self, d): ...
    def store_document(self, d): ...
    def index_document(self, d): ...
    def emit_event(self, d): ...
```

The caller now knows the full ingest pipeline. Tests must mock all five methods.

**Deep** (same logic, smaller surface):

```python
class Catalog:
    def ingest(self, document: Document) -> str:
        # normalize → deduplicate → store → index → emit
        # all hidden
```

Tests assert one behavior: *after `ingest`, the document is retrievable by id.* The pipeline can be re-ordered, parallelized, or rewritten without touching a single test.
