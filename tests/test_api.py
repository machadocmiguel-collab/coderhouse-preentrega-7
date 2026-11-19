import os
os.environ['TRACING_ENABLED'] = 'false'

import asyncio
import time
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from redis import Redis

from app.main import create_app
from app.graph import Decision
from app.agents.analyst_agent import CalculationPlan
from app.settings import API_TOKEN, REDIS_URL

HEADERS = {'X-API-Key': API_TOKEN}
QUESTION = 'Investigá chunking y calculá ventanas para 2000 tokens, tamaño 500 y solapamiento 50.'


class Model:
    def with_structured_output(self, schema, **kwargs):
        class Runnable:
            async def ainvoke(self, messages):
                await asyncio.sleep(.01)
                text = str(messages)
                if 'fallo-controlado' in text:
                    raise RuntimeError('Fallo de modelo en prueba')
                if schema is Decision:
                    return Decision(next_agent='finish', instruction='Completá la evidencia pendiente', reason='Prueba de validación')
                if schema is CalculationPlan:
                    return CalculationPlan(total_tokens=2000, chunk_size=500, overlap=50)
                return schema(query='chunking tokens dimensionamiento')
        return Runnable()


@pytest.fixture
def setup():
    prefix = 'test7-' + uuid4().hex
    app = create_app(model=Model(), prefix=prefix)
    with TestClient(app, headers=HEADERS) as client:
        yield client, prefix
    with Redis.from_url(REDIS_URL) as redis:
        keys = list(redis.scan_iter(prefix + ':*'))
        if keys:
            redis.delete(*keys)


def wait(client, job_id, statuses=('DONE', 'FAILED', 'WAITING_APPROVAL', 'REJECTED')):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        job = client.get('/tasks/' + job_id).json()
        if job['status'] in statuses:
            return job
        time.sleep(.02)
    raise AssertionError('El worker no terminó a tiempo')


def test_async_api_and_redis_checkpoints(setup):
    client, prefix = setup
    started = time.monotonic()
    responses = [client.post('/tasks', json={'request': QUESTION}) for _ in range(5)]
    assert all(r.status_code == 202 for r in responses)
    assert time.monotonic() - started < 2
    for response in responses:
        job_id = response.json()['job_id']
        result = wait(client, job_id)
        assert result['status'] == 'DONE'
        assert result['result']['analysis']['chunks'] == 5
    with Redis.from_url(REDIS_URL) as redis:
        assert list(redis.scan_iter(prefix + ':cp:*:index'))


def test_approval_persists_across_api_restart():
    prefix = 'test7-' + uuid4().hex
    with TestClient(create_app(model=Model(), prefix=prefix), headers=HEADERS) as client:
        job_id = client.post('/tasks', json={'request': QUESTION, 'action': 'publish_report'}).json()['job_id']
        assert wait(client, job_id)['status'] == 'WAITING_APPROVAL'
        assert client.get(f'/tasks/{job_id}/report').status_code == 404
    with TestClient(create_app(model=Model(), prefix=prefix), headers=HEADERS) as restarted:
        assert restarted.get('/tasks/' + job_id).json()['status'] == 'WAITING_APPROVAL'
        assert restarted.post(f'/tasks/{job_id}/approve', json={'approved': True}).status_code == 202
        result = wait(restarted, job_id)
        assert result['status'] == 'DONE'
        assert result['result']['published']
        assert restarted.get(f'/tasks/{job_id}/report').status_code == 200
        assert restarted.post(f'/tasks/{job_id}/approve', json={'approved': True}).status_code == 409
    with Redis.from_url(REDIS_URL) as redis:
        keys = list(redis.scan_iter(prefix + ':*'))
        redis.delete(*keys)


def test_rejection_never_publishes(setup):
    client, _ = setup
    job_id = client.post('/tasks', json={'request': QUESTION, 'action': 'publish_report'}).json()['job_id']
    assert wait(client, job_id)['status'] == 'WAITING_APPROVAL'
    client.post(f'/tasks/{job_id}/approve', json={'approved': False})
    assert wait(client, job_id)['status'] == 'REJECTED'
    assert client.get(f'/tasks/{job_id}/report').status_code == 404


def test_worker_failure_and_endpoint_validation(setup):
    client, _ = setup
    job_id = client.post('/tasks', json={'request': QUESTION + ' fallo-controlado'}).json()['job_id']
    assert wait(client, job_id)['status'] == 'FAILED'
    assert client.get('/tasks/inexistente').status_code == 404
    assert client.post('/tasks', json={'request': 'x'}).status_code == 422
    assert client.post('/tasks', json={'request': QUESTION}, headers={'X-API-Key': 'incorrecta'}).status_code == 401
