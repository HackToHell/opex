-- Deterministic seed data for integration tests.
-- All timestamps are fixed (2025-01-15) so assertions are exact.
-- 7 traces, 26 spans, 5 services.

-- =============================================================================
-- Trace 1: Successful user login flow (4 spans, 350ms root)
-- frontend -> api-gateway -> user-service -> DB
-- =============================================================================

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 10:00:00.000000000',
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    '1000000000000001',
    '',
    '',
    'GET /login',
    'SPAN_KIND_SERVER',
    'frontend',
    {'service.name': 'frontend', 'service.version': '1.2.0', 'deployment.environment': 'production', 'host.name': 'frontend-pod-abc'},
    'github.com/frontend/http',
    '0.1.0',
    {'http.method': 'GET', 'http.url': '/login', 'http.status_code': '200', 'http.scheme': 'https', 'user.agent': 'Mozilla/5.0'},
    350000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 10:00:00.005000000',
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    '1000000000000002',
    '1000000000000001',
    '',
    'POST /api/v1/auth',
    'SPAN_KIND_SERVER',
    'api-gateway',
    {'service.name': 'api-gateway', 'service.version': '2.0.1', 'deployment.environment': 'production', 'host.name': 'gateway-pod-xyz'},
    'github.com/gateway/grpc',
    '0.2.0',
    {'http.method': 'POST', 'http.url': '/api/v1/auth', 'http.status_code': '200', 'rpc.system': 'grpc'},
    300000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 10:00:00.010000000',
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    '1000000000000003',
    '1000000000000002',
    '',
    'ValidateCredentials',
    'SPAN_KIND_INTERNAL',
    'user-service',
    {'service.name': 'user-service', 'service.version': '1.5.3', 'deployment.environment': 'production', 'host.name': 'user-pod-123'},
    'github.com/user/logic',
    '0.3.0',
    {'db.system': 'postgresql', 'db.statement': 'SELECT * FROM users WHERE email = $1', 'db.name': 'users_db', 'enduser.id': 'user-42'},
    200000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 10:00:00.015000000',
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    '1000000000000004',
    '1000000000000003',
    '',
    'pg.query',
    'SPAN_KIND_CLIENT',
    'user-service',
    {'service.name': 'user-service', 'service.version': '1.5.3', 'deployment.environment': 'production', 'host.name': 'user-pod-123'},
    'github.com/user/db',
    '0.3.0',
    {'db.system': 'postgresql', 'db.statement': 'SELECT id, email, password_hash FROM users WHERE email = $1', 'db.name': 'users_db', 'db.operation': 'SELECT', 'net.peer.name': 'db.internal', 'net.peer.port': '5432'},
    50000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

-- =============================================================================
-- Trace 2: Failed order creation (5 spans, 2500ms root, ERROR)
-- frontend -> api-gateway -> order-service -> payment-service (ERROR)
-- =============================================================================

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:00:00.000000000',
    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    '2000000000000001',
    '',
    '',
    'POST /orders',
    'SPAN_KIND_SERVER',
    'frontend',
    {'service.name': 'frontend', 'service.version': '1.2.0', 'deployment.environment': 'production', 'host.name': 'frontend-pod-def'},
    'github.com/frontend/http',
    '0.1.0',
    {'http.method': 'POST', 'http.url': '/orders', 'http.status_code': '500', 'http.scheme': 'https'},
    2500000000,
    'STATUS_CODE_ERROR',
    'Internal Server Error',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:00:00.003000000',
    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    '2000000000000002',
    '2000000000000001',
    '',
    'POST /api/v1/orders',
    'SPAN_KIND_SERVER',
    'api-gateway',
    {'service.name': 'api-gateway', 'service.version': '2.0.1', 'deployment.environment': 'production', 'host.name': 'gateway-pod-xyz'},
    'github.com/gateway/grpc',
    '0.2.0',
    {'http.method': 'POST', 'http.url': '/api/v1/orders', 'http.status_code': '500', 'rpc.system': 'grpc'},
    2400000000,
    'STATUS_CODE_ERROR',
    'upstream error',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:00:00.010000000',
    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    '2000000000000003',
    '2000000000000002',
    '',
    'CreateOrder',
    'SPAN_KIND_INTERNAL',
    'order-service',
    {'service.name': 'order-service', 'service.version': '3.1.0', 'deployment.environment': 'production', 'host.name': 'order-pod-456'},
    'github.com/order/logic',
    '0.4.0',
    {'order.id': 'ord-12345', 'order.total': '149.99', 'order.items': '3'},
    2000000000,
    'STATUS_CODE_ERROR',
    'payment failed',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:00:00.015000000',
    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    '2000000000000004',
    '2000000000000003',
    '',
    'pg.query',
    'SPAN_KIND_CLIENT',
    'order-service',
    {'service.name': 'order-service', 'service.version': '3.1.0', 'deployment.environment': 'production', 'host.name': 'order-pod-456'},
    'github.com/order/db',
    '0.4.0',
    {'db.system': 'postgresql', 'db.statement': 'INSERT INTO orders (id, user_id, total) VALUES ($1, $2, $3)', 'db.name': 'orders_db', 'db.operation': 'INSERT'},
    30000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:00:00.050000000',
    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    '2000000000000005',
    '2000000000000003',
    '',
    'ProcessPayment',
    'SPAN_KIND_CLIENT',
    'payment-service',
    {'service.name': 'payment-service', 'service.version': '1.0.0', 'deployment.environment': 'production', 'host.name': 'payment-pod-789'},
    'github.com/payment/stripe',
    '0.1.0',
    {'payment.provider': 'stripe', 'payment.amount': '149.99', 'payment.currency': 'USD', 'error.type': 'PaymentDeclined'},
    1800000000,
    'STATUS_CODE_ERROR',
    'card declined',
    ['2025-01-15 11:00:01.800000000'],
    ['exception'],
    [{'exception.type': 'PaymentDeclinedError', 'exception.message': 'Card ending in 4242 was declined', 'exception.stacktrace': 'at ProcessPayment() payment.go:42\nat HandleOrder() order.go:88'}],
    [],
    [],
    [],
    []
);

-- =============================================================================
-- Trace 3: Successful product listing with cache hit (3 spans, 15ms root)
-- frontend -> api-gateway -> redis
-- =============================================================================

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:30:00.000000000',
    'cccccccccccccccccccccccccccccccc',
    '3000000000000001',
    '',
    '',
    'GET /products',
    'SPAN_KIND_SERVER',
    'frontend',
    {'service.name': 'frontend', 'service.version': '1.2.0', 'deployment.environment': 'production', 'host.name': 'frontend-pod-abc'},
    'github.com/frontend/http',
    '0.1.0',
    {'http.method': 'GET', 'http.url': '/products', 'http.status_code': '200', 'http.scheme': 'https'},
    15000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:30:00.001000000',
    'cccccccccccccccccccccccccccccccc',
    '3000000000000002',
    '3000000000000001',
    '',
    'GET /api/v1/products',
    'SPAN_KIND_SERVER',
    'api-gateway',
    {'service.name': 'api-gateway', 'service.version': '2.0.1', 'deployment.environment': 'production', 'host.name': 'gateway-pod-xyz'},
    'github.com/gateway/grpc',
    '0.2.0',
    {'http.method': 'GET', 'http.url': '/api/v1/products', 'http.status_code': '200', 'cache.hit': 'true'},
    10000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:30:00.002000000',
    'cccccccccccccccccccccccccccccccc',
    '3000000000000003',
    '3000000000000002',
    '',
    'redis.GET',
    'SPAN_KIND_CLIENT',
    'api-gateway',
    {'service.name': 'api-gateway', 'service.version': '2.0.1', 'deployment.environment': 'production', 'host.name': 'gateway-pod-xyz'},
    'github.com/gateway/cache',
    '0.2.0',
    {'db.system': 'redis', 'db.operation': 'GET', 'db.statement': 'GET products:all', 'net.peer.name': 'redis.internal', 'net.peer.port': '6379'},
    2000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

-- =============================================================================
-- Trace 4: Slow order query with DB timeout (4 spans, 4800ms root)
-- frontend -> api-gateway -> order-service -> DB (slow)
-- =============================================================================

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:45:00.000000000',
    'dddddddddddddddddddddddddddddd',
    '4000000000000001',
    '',
    '',
    'GET /orders/history',
    'SPAN_KIND_SERVER',
    'frontend',
    {'service.name': 'frontend', 'service.version': '1.2.0', 'deployment.environment': 'staging', 'host.name': 'frontend-pod-staging-1'},
    'github.com/frontend/http',
    '0.1.0',
    {'http.method': 'GET', 'http.url': '/orders/history', 'http.status_code': '200', 'http.scheme': 'https'},
    4800000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:45:00.005000000',
    'dddddddddddddddddddddddddddddd',
    '4000000000000002',
    '4000000000000001',
    '',
    'GET /api/v1/orders',
    'SPAN_KIND_SERVER',
    'api-gateway',
    {'service.name': 'api-gateway', 'service.version': '2.0.1', 'deployment.environment': 'staging', 'host.name': 'gateway-pod-staging-1'},
    'github.com/gateway/grpc',
    '0.2.0',
    {'http.method': 'GET', 'http.url': '/api/v1/orders', 'http.status_code': '200'},
    4700000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:45:00.010000000',
    'dddddddddddddddddddddddddddddd',
    '4000000000000003',
    '4000000000000002',
    '',
    'ListOrders',
    'SPAN_KIND_INTERNAL',
    'order-service',
    {'service.name': 'order-service', 'service.version': '3.1.0', 'deployment.environment': 'staging', 'host.name': 'order-pod-staging-1'},
    'github.com/order/logic',
    '0.4.0',
    {'order.query.user_id': 'user-42', 'order.query.limit': '100'},
    4500000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:45:00.015000000',
    'dddddddddddddddddddddddddddddd',
    '4000000000000004',
    '4000000000000003',
    '',
    'pg.query',
    'SPAN_KIND_CLIENT',
    'order-service',
    {'service.name': 'order-service', 'service.version': '3.1.0', 'deployment.environment': 'staging', 'host.name': 'order-pod-staging-1'},
    'github.com/order/db',
    '0.4.0',
    {'db.system': 'postgresql', 'db.statement': 'SELECT * FROM orders WHERE user_id = $1 ORDER BY created_at DESC LIMIT 100', 'db.name': 'orders_db', 'db.operation': 'SELECT'},
    4200000000,
    'STATUS_CODE_OK',
    '',
    ['2025-01-15 11:45:03.000000000'],
    ['db.slow_query'],
    [{'db.slow_query.threshold_ms': '1000', 'db.slow_query.actual_ms': '4200'}],
    [],
    [],
    [],
    []
);

-- =============================================================================
-- Trace 5: Simple health check (1 span, 1ms)
-- frontend only
-- =============================================================================

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:55:00.000000000',
    'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    '5000000000000001',
    '',
    '',
    'GET /healthz',
    'SPAN_KIND_SERVER',
    'frontend',
    {'service.name': 'frontend', 'service.version': '1.2.0', 'deployment.environment': 'production', 'host.name': 'frontend-pod-abc'},
    'github.com/frontend/http',
    '0.1.0',
    {'http.method': 'GET', 'http.url': '/healthz', 'http.status_code': '200'},
    1000000,
    'STATUS_CODE_UNSET',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

-- =============================================================================
-- Trace 6: Inter-service call with links (2 spans, 500ms)
-- order-service -> payment-service, linked to trace bbbb
-- =============================================================================

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:15:00.000000000',
    'ffffffffffffffffffffffffffffffff',
    '6000000000000001',
    '',
    '',
    'RetryPayment',
    'SPAN_KIND_SERVER',
    'order-service',
    {'service.name': 'order-service', 'service.version': '3.1.0', 'deployment.environment': 'production', 'host.name': 'order-pod-456'},
    'github.com/order/retry',
    '0.4.0',
    {'order.id': 'ord-12345', 'retry.attempt': '1', 'http.method': 'POST'},
    500000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    ['bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'],
    ['2000000000000003'],
    [''],
    [{'link.reason': 'retry of failed payment'}]
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:15:00.010000000',
    'ffffffffffffffffffffffffffffffff',
    '6000000000000002',
    '6000000000000001',
    '',
    'ProcessPayment',
    'SPAN_KIND_CLIENT',
    'payment-service',
    {'service.name': 'payment-service', 'service.version': '1.0.0', 'deployment.environment': 'production', 'host.name': 'payment-pod-789'},
    'github.com/payment/stripe',
    '0.1.0',
    {'payment.provider': 'stripe', 'payment.amount': '149.99', 'payment.currency': 'USD', 'http.method': 'POST', 'http.status_code': '200'},
    400000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

-- =============================================================================
-- Trace 7: Dashboard page load with parallel calls (7 spans, 800ms root)
-- frontend -> (api-gateway x2) -> user-service + order-service
-- =============================================================================

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:50:00.000000000',
    '11111111111111111111111111111111',
    '7000000000000001',
    '',
    '',
    'GET /dashboard',
    'SPAN_KIND_SERVER',
    'frontend',
    {'service.name': 'frontend', 'service.version': '1.2.0', 'deployment.environment': 'production', 'host.name': 'frontend-pod-ghi'},
    'github.com/frontend/http',
    '0.1.0',
    {'http.method': 'GET', 'http.url': '/dashboard', 'http.status_code': '200', 'http.scheme': 'https', 'component': 'dashboard'},
    800000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:50:00.002000000',
    '11111111111111111111111111111111',
    '7000000000000002',
    '7000000000000001',
    '',
    'GET /api/v1/profile',
    'SPAN_KIND_CLIENT',
    'frontend',
    {'service.name': 'frontend', 'service.version': '1.2.0', 'deployment.environment': 'production', 'host.name': 'frontend-pod-ghi'},
    'github.com/frontend/http',
    '0.1.0',
    {'http.method': 'GET', 'http.url': '/api/v1/profile', 'http.status_code': '200', 'http.scheme': 'https'},
    200000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:50:00.002000000',
    '11111111111111111111111111111111',
    '7000000000000003',
    '7000000000000001',
    '',
    'GET /api/v1/orders/recent',
    'SPAN_KIND_CLIENT',
    'frontend',
    {'service.name': 'frontend', 'service.version': '1.2.0', 'deployment.environment': 'production', 'host.name': 'frontend-pod-ghi'},
    'github.com/frontend/http',
    '0.1.0',
    {'http.method': 'GET', 'http.url': '/api/v1/orders/recent', 'http.status_code': '200', 'http.scheme': 'https'},
    750000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:50:00.004000000',
    '11111111111111111111111111111111',
    '7000000000000004',
    '7000000000000002',
    '',
    'GET /api/v1/profile',
    'SPAN_KIND_SERVER',
    'api-gateway',
    {'service.name': 'api-gateway', 'service.version': '2.0.1', 'deployment.environment': 'production', 'host.name': 'gateway-pod-xyz'},
    'github.com/gateway/grpc',
    '0.2.0',
    {'http.method': 'GET', 'http.url': '/api/v1/profile', 'http.status_code': '200'},
    180000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:50:00.004000000',
    '11111111111111111111111111111111',
    '7000000000000005',
    '7000000000000003',
    '',
    'GET /api/v1/orders/recent',
    'SPAN_KIND_SERVER',
    'api-gateway',
    {'service.name': 'api-gateway', 'service.version': '2.0.1', 'deployment.environment': 'production', 'host.name': 'gateway-pod-xyz'},
    'github.com/gateway/grpc',
    '0.2.0',
    {'http.method': 'GET', 'http.url': '/api/v1/orders/recent', 'http.status_code': '200'},
    700000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:50:00.006000000',
    '11111111111111111111111111111111',
    '7000000000000006',
    '7000000000000004',
    '',
    'GetUserProfile',
    'SPAN_KIND_INTERNAL',
    'user-service',
    {'service.name': 'user-service', 'service.version': '1.5.3', 'deployment.environment': 'production', 'host.name': 'user-pod-123'},
    'github.com/user/logic',
    '0.3.0',
    {'enduser.id': 'user-42', 'db.system': 'postgresql'},
    150000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);

INSERT INTO otel.otel_traces VALUES (
    '2025-01-15 11:50:00.006000000',
    '11111111111111111111111111111111',
    '7000000000000007',
    '7000000000000005',
    '',
    'ListRecentOrders',
    'SPAN_KIND_INTERNAL',
    'order-service',
    {'service.name': 'order-service', 'service.version': '3.1.0', 'deployment.environment': 'production', 'host.name': 'order-pod-456'},
    'github.com/order/logic',
    '0.4.0',
    {'order.query.limit': '10', 'order.query.user_id': 'user-42'},
    650000000,
    'STATUS_CODE_OK',
    '',
    [],
    [],
    [],
    [],
    [],
    [],
    []
);
