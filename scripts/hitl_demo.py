import asyncio
import json
import os
from pathlib import Path

import httpx


async def main():
    async with httpx.AsyncClient(base_url=os.getenv('API_URL', 'http://127.0.0.1:8007'),
                                 headers={'X-API-Key': os.getenv('API_TOKEN', 'local-demo')}, timeout=30) as client:
        response = await client.post('/tasks', json={
            'request': 'Investigá chunking: documento de 2000 tokens, ventanas de 500 y solapamiento 50.',
            'action': 'publish_report', 'batch': 'hitl',
        })
        response.raise_for_status()
        job_id = response.json()['job_id']
        waiting = None
        for _ in range(300):
            job = (await client.get('/tasks/' + job_id)).json()
            if job['status'] == 'FAILED':
                raise RuntimeError(job)
            if job['status'] == 'WAITING_APPROVAL':
                waiting = job
                break
            await asyncio.sleep(2)
        assert waiting, 'No llegó a la pausa de aprobación'
        assert (await client.get(f'/tasks/{job_id}/report')).status_code == 404
        print('WAITING_APPROVAL: informe inexistente antes de aprobar.', flush=True)
        approval = await client.post(f'/tasks/{job_id}/approve', json={'approved': True})
        approval.raise_for_status()
        for _ in range(30):
            job = (await client.get('/tasks/' + job_id)).json()
            if job['status'] == 'DONE':
                break
            if job['status'] == 'FAILED':
                raise RuntimeError(job)
            await asyncio.sleep(1)
        assert job['status'] == 'DONE' and job['result']['published']
        report = await client.get(f'/tasks/{job_id}/report')
        report.raise_for_status()
        evidence = {'waiting': waiting, 'approval': approval.json(), 'completed': job, 'report': report.json()}
        Path(__file__).resolve().parents[1].joinpath('hitl_result.json').write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2), encoding='utf-8')
        print('DONE: informe publicado únicamente después del POST de aprobación.', flush=True)


if __name__ == '__main__':
    asyncio.run(main())
