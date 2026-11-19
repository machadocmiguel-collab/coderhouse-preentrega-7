import asyncio
import json
import os
import time
from pathlib import Path

import httpx

BASE = os.getenv('API_URL', 'http://127.0.0.1:8007')
ROOT = Path(__file__).resolve().parents[1]


async def main():
    async with httpx.AsyncClient(base_url=BASE, headers={'X-API-Key': os.getenv('API_TOKEN', 'local-demo')}, timeout=30) as client:
        start = time.perf_counter()

        async def submit(index):
            response = await client.post('/tasks', json={
                'request': f'Investigá chunking con solapamiento. Calculá las ventanas fijas para {2000 + index * 100} tokens, tamaño 500 y solapamiento 50. Citá fuentes.',
                'batch': 'carga5',
            })
            response.raise_for_status()
            return {**response.json(), 'accepted_after_seconds': time.perf_counter() - start}

        accepted = await asyncio.gather(*(submit(i) for i in range(5)))
        print('Cinco peticiones concurrentes aceptadas:', json.dumps(accepted, indent=2), flush=True)
        results = []
        for submitted in accepted:
            deadline = time.monotonic() + 1800
            while time.monotonic() < deadline:
                response = await client.get('/tasks/' + submitted['job_id'])
                response.raise_for_status()
                job = response.json()
                if job['status'] in {'DONE', 'FAILED'}:
                    if job['status'] != 'DONE':
                        raise RuntimeError(job)
                    results.append(job)
                    print('Finalizó:', job['job_id'], job['result']['analysis'], flush=True)
                    break
                await asyncio.sleep(2)
            else:
                raise TimeoutError(submitted['job_id'])
        output = {'accepted': accepted, 'jobs': results}
        (ROOT / 'load_results.json').write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
        print('Prueba completada: 5/5 DONE', flush=True)


if __name__ == '__main__':
    asyncio.run(main())
