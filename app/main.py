import asyncio
import contextlib
import secrets
from contextlib import asynccontextmanager
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from app import settings
from app.checkpoints import AsyncRedisSaver
from app.graph import build_graph
from app.storage import JobStore
from app.worker import Worker


class TaskInput(BaseModel):
    request: str = Field(min_length=10, max_length=4000)
    action: Literal['analyze', 'publish_report'] = 'analyze'
    batch: str = Field(default='manual', max_length=80)


class ApprovalInput(BaseModel):
    approved: bool


async def authenticate(x_api_key: str = Header(default='')):
    if not secrets.compare_digest(x_api_key, settings.API_TOKEN):
        raise HTTPException(401, 'Clave de API incorrecta')


def create_app(model=None, redis_url=None, prefix=None):
    @asynccontextmanager
    async def lifespan(app):
        redis = Redis.from_url(redis_url or settings.REDIS_URL, decode_responses=False)
        await redis.ping()
        store = JobStore(redis, prefix or settings.PREFIX)
        saver = AsyncRedisSaver(redis, prefix or settings.PREFIX)
        graph = build_graph(model=model, checkpointer=saver, redis=redis, prefix=prefix or settings.PREFIX)
        await store.recover()
        app.state.store, app.state.graph = store, graph
        workers = [asyncio.create_task(Worker(store, graph).run()) for _ in range(settings.WORKERS)]
        try:
            yield
        finally:
            for task in workers:
                task.cancel()
            for task in workers:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await redis.aclose()

    app = FastAPI(title='Orquestador multiagente', version='1.0', lifespan=lifespan,
                  telemetry={'tracing': False})

    @app.get('/health')
    async def health():
        return {'redis': bool(await app.state.store.redis.ping())}

    @app.post('/tasks', status_code=202, dependencies=[Depends(authenticate)])
    async def create_task(body: TaskInput):
        job_id = str(uuid4())
        await app.state.store.create(job_id, body.model_dump())
        return {'job_id': job_id, 'status': 'PENDING'}

    @app.get('/tasks/{job_id}', dependencies=[Depends(authenticate)])
    async def get_task(job_id: str):
        job = await app.state.store.get(job_id)
        if job is None:
            raise HTTPException(404, 'Trabajo inexistente')
        return job

    @app.post('/tasks/{job_id}/approve', status_code=202, dependencies=[Depends(authenticate)])
    async def approve(job_id: str, body: ApprovalInput):
        if await app.state.store.get(job_id) is None:
            raise HTTPException(404, 'Trabajo inexistente')
        if not await app.state.store.approve(job_id, body.approved):
            raise HTTPException(409, 'El trabajo no está esperando aprobación')
        return {'job_id': job_id, 'status': 'PENDING', 'approved': body.approved}

    @app.get('/tasks/{job_id}/report', dependencies=[Depends(authenticate)])
    async def get_report(job_id: str):
        import json
        raw = await app.state.store.redis.get(f'{app.state.store.prefix}:report:{job_id}')
        if raw is None:
            raise HTTPException(404, 'No existe un informe publicado')
        return json.loads(raw)

    return app


app = create_app()
