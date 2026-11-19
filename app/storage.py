import json
from datetime import datetime, timezone


def now():
    return datetime.now(timezone.utc).isoformat()


class JobStore:
    def __init__(self, redis, prefix):
        self.redis, self.prefix = redis, prefix
        self.queue = prefix + ':pending'
        self.processing = prefix + ':processing'

    def key(self, job_id):
        return f'{self.prefix}:job:{job_id}'

    async def create(self, job_id, payload):
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self.key(job_id), mapping={'status': 'PENDING', 'payload': json.dumps(payload),
                                                'created_at': now(), 'updated_at': now()})
            pipe.lpush(self.queue, job_id)
            await pipe.execute()

    async def get(self, job_id):
        data = {k.decode(): v.decode() for k, v in (await self.redis.hgetall(self.key(job_id))).items()}
        if not data:
            return None
        for field in ['payload', 'result', 'interrupts', 'approved']:
            if field in data:
                data[field] = json.loads(data[field])
        return {'job_id': job_id, **data}

    async def update(self, job_id, **changes):
        encoded = {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list, bool)) else str(v)
                   for k, v in changes.items()}
        await self.redis.hset(self.key(job_id), mapping={**encoded, 'updated_at': now()})

    async def approve(self, job_id, approved):
        script = '''
        if redis.call('HGET', KEYS[1], 'status') ~= 'WAITING_APPROVAL' then return 0 end
        redis.call('HSET', KEYS[1], 'status', 'PENDING', 'approved', ARGV[1], 'resume', '1', 'updated_at', ARGV[3])
        redis.call('LPUSH', KEYS[2], ARGV[2])
        return 1
        '''
        return bool(await self.redis.eval(script, 2, self.key(job_id), self.queue,
                                         json.dumps(approved), job_id, now()))

    async def recover(self):
        while job_id := await self.redis.rpoplpush(self.processing, self.queue):
            await self.update(job_id.decode(), status='PENDING')
