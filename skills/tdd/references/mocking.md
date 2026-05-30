# When to Mock

Mock at **system boundaries** only:

- External APIs (payment providers, email, maps/weather services, any third-party REST API)
- External databases — sometimes; prefer a real test DB or in-memory equivalent (sqlite for tests, etc.)
- Time and randomness (`datetime.now()`, `random`, UUIDs)
- File system — sometimes; usually `tmp_path` fixture is better
- The clock and external schedulers

Don't mock:

- Your own classes or modules
- Internal collaborators (helper functions you wrote)
- Anything you control and can run in-process

## Designing for mockability

At system boundaries, design interfaces that are easy to mock.

### 1. Use dependency injection

Pass external dependencies in rather than creating them inside the function:

```python
# Easy to mock
def process_payment(order, payment_client):
    return payment_client.charge(order.total)

# Hard to mock
def process_payment(order):
    client = StripeClient(os.environ["STRIPE_KEY"])
    return client.charge(order.total)
```

Tests can pass a `FakePaymentClient`. Production passes a real `StripeClient`.

### 2. Prefer SDK-style interfaces over generic fetchers

Specific functions per operation, not one generic do-it-all:

```python
# GOOD: each method independently mockable
class CatalogApi:
    def get_item(self, item_id: str) -> Item: ...
    def list_items(self, topic: str) -> list[Item]: ...
    def add_item(self, item: Item) -> str: ...

# BAD: mocking requires conditional logic inside the mock
class CatalogApi:
    def fetch(self, endpoint: str, params: dict): ...
```

The SDK approach means:

- Each mock returns one specific shape
- No conditional logic in test setup
- Easier to see which endpoints a test exercises
- Type safety per endpoint

### 3. Prefer protocols over concrete classes for the boundary

In Python, define a `Protocol` for the dependency. Tests implement it; production uses the real class.

```python
from typing import Protocol

class PaymentClient(Protocol):
    def charge(self, amount: int) -> ChargeResult: ...

# Production
def process_payment(order, client: PaymentClient = StripeClient()):
    ...

# Test
class FakePaymentClient:
    def charge(self, amount: int) -> ChargeResult:
        return ChargeResult(status="confirmed", id="fake-1")
```

No `Mock` library needed. The fake is a real object that satisfies the protocol.

## Anti-patterns

- **`mocker.patch("mymodule.helper_function")`** — you're mocking your own code. The test now breaks if you rename `helper_function`, even if behavior is identical.
- **Mocking the database driver** — usually wrong. Use a real test DB (sqlite-in-memory for unit tests, a docker-compose Postgres for integration).
- **Asserting `mock.called_with(...)`** on every test — this turns the test into a record of *how* the function works, not *what* it does. Asserting on call shape is sometimes correct (at a true boundary), but it's a smell when overused.
