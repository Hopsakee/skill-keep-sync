# Good and Bad Tests

## Good Tests

**Integration-style**: Test through real interfaces, not mocks of internal parts.

```python
# GOOD: Tests observable behavior
def test_catalog_ingests_document_with_id():
    catalog = Catalog(storage=test_storage)
    catalog.ingest(Document(id="doc-123", title="Test"))
    found = catalog.find_by_id("doc-123")
    assert found.title == "Test"
```

Characteristics:

- Tests behavior users/callers care about
- Uses public API only
- Survives internal refactors
- Describes WHAT, not HOW
- One logical assertion per test

## Bad Tests

**Implementation-detail tests**: Coupled to internal structure.

```python
# BAD: Tests implementation details
def test_ingest_calls_normalize_then_persist(mocker):
    normalize = mocker.patch("catalog._normalize")
    persist = mocker.patch("catalog._persist")
    catalog.ingest(document)
    normalize.assert_called_once()
    persist.assert_called_once()
```

Red flags:

- Mocking internal collaborators (anything your own code controls)
- Testing private methods (the `_` prefix in Python signals "don't test me directly")
- Asserting on call counts/order of internal helpers
- Test breaks when refactoring without behavior change
- Test name describes HOW not WHAT
- Verifying through external means instead of the interface

```python
# BAD: Bypasses interface to verify
def test_create_user_writes_to_db(db_conn):
    create_user(name="Alice")
    row = db_conn.execute("SELECT * FROM users WHERE name = ?", ("Alice",)).fetchone()
    assert row is not None

# GOOD: Verifies through interface
def test_create_user_makes_user_retrievable():
    user = create_user(name="Alice")
    retrieved = get_user(user.id)
    assert retrieved.name == "Alice"
```

## Naming

A test name is a specification. Read aloud, it should describe a capability.

- `test_user_can_checkout_with_valid_cart` â describes capability â
- `test_checkout` â describes the function being tested, not the capability â
- `test_checkout_returns_confirmed_status` â describes return shape, not user-facing meaning â

If you can't write a good name, you probably don't yet understand what behavior you're testing.

## One assertion per test (logically)

A "logical assertion" can be multiple `assert` statements that all verify the same outcome:

```python
def test_user_can_checkout_with_valid_cart():
    result = checkout(cart, payment)
    assert result.status == "confirmed"  # OK: all three asserts
    assert result.order_id is not None    # describe ONE outcome
    assert result.total == cart.total     # (a successful checkout)
```

What's wrong is bundling multiple unrelated outcomes:

```python
def test_checkout_works():
    # checks success path AND failure path AND validation
    # â if this fails, you don't know which capability is broken
```

## Pytest specifics

- **Fixtures over setUp/tearDown** â pytest fixtures compose better
- **`parametrize` for multiple inputs to the same behavior** â not for multiple behaviors
- **`pytest-asyncio`** for async code; don't mix sync and async in one test
- **Don't use `unittest.mock.patch` on your own modules** â that's the implementation-coupling smell
