import asyncio
import json

from langchain_core.messages import HumanMessage
from langgraph.types import Command
from openinference.instrumentation import using_metadata
from opentelemetry.trace import get_current_span

from app.observability import tracer, provider
from app.settings import JOB_TIMEOUT


class Worker:
    def __init__(self, store, graph):
        self.store, self.graph = store, graph

    @tracer.chain(name='ejecucion_job')
    async def execute(self, job_id):
        span = get_current_span()
        await self.store.update(job_id, status='RUNNING', trace_id=f'{span.get_span_context().trace_id:032x}')
        job = await self.store.get(job_id)
        config = {'configurable': {'thread_id': job_id}, 'recursion_limit': 24}
        if job.get('resume') == '1':
            inputs = Command(resume=job['approved'])
        else:
            snapshot = await self.graph.aget_state(config)
            inputs = None if snapshot.values else {
                'job_id': job_id, 'action': job['payload']['action'],
                'request': job['payload']['request'],
                'messages': [HumanMessage(content=job['payload']['request'])],
                'research': {}, 'analysis': {}, 'contributions': [], 'decisions': [],
                'steps': 0, 'feedback': '', 'task_completed': False, 'final_answer': '',
                'approved': False, 'published': False,
            }
        with using_metadata({'job_id': job_id, 'batch': job['payload'].get('batch', 'manual')}):
            async with asyncio.timeout(JOB_TIMEOUT):
                result = await self.graph.ainvoke(inputs, config)
        snapshot = await self.graph.aget_state(config)
        interruptions = [i.value for task in snapshot.tasks for i in task.interrupts]
        if interruptions:
            await self.store.update(job_id, status='WAITING_APPROVAL', interrupts=interruptions)
        else:
            if not result.get('task_completed') and job.get('approved') is not False:
                raise RuntimeError(result.get('final_answer', 'Sin resultado validado'))
            status = 'REJECTED' if job.get('approved') is False else 'DONE'
            clean = {k: v for k, v in result.items() if k != 'messages'}
            await self.store.update(job_id, status=status, result=clean, interrupts=[])
        await asyncio.to_thread(provider.force_flush)

    async def run(self):
        while True:
            raw = await self.store.redis.brpoplpush(self.store.queue, self.store.processing, timeout=1)
            if raw is None:
                continue
            job_id = raw.decode()
            try:
                await self.execute(job_id)
            except asyncio.CancelledError:
                # La lista processing permite retomar el checkpoint al reiniciar.
                raise
            except Exception as exc:
                await self.store.update(job_id, status='FAILED', error=f'{type(exc).__name__}: {str(exc)[:400]}')
            else:
                await self.store.redis.lrem(self.store.processing, 1, raw)
                continue
            await self.store.redis.lrem(self.store.processing, 1, raw)
