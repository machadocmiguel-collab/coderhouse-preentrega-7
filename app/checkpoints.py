import base64
import json

from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple, WRITES_IDX_MAP, get_checkpoint_metadata


class AsyncRedisSaver(BaseCheckpointSaver):
    """Checkpoints completos en Redis estándar; no requiere módulos JSON/Search."""

    def __init__(self, redis, prefix='pre7'):
        super().__init__()
        self.redis = redis
        self.prefix = prefix + ':cp:'

    def _base(self, config):
        c = config['configurable']
        encode = lambda s: base64.urlsafe_b64encode(s.encode()).decode()
        return self.prefix + encode(c['thread_id']) + ':' + encode(c.get('checkpoint_ns', ''))

    def _pack(self, value):
        kind, data = self.serde.dumps_typed(value)
        return json.dumps([kind, base64.b64encode(data).decode()])

    def _unpack(self, value):
        kind, data = json.loads(value)
        return self.serde.loads_typed((kind, base64.b64decode(data)))

    async def aput(self, config, checkpoint, metadata, new_versions):
        base = self._base(config)
        cid = checkpoint['id']
        record = {'checkpoint': checkpoint,
                  'metadata': get_checkpoint_metadata(config, metadata),
                  'parent': config['configurable'].get('checkpoint_id')}
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.set(base + ':' + cid, self._pack(record))
            pipe.zadd(base + ':index', {cid: 0})
            await pipe.execute()
        return {'configurable': {**config['configurable'], 'checkpoint_id': cid}}

    async def aget_tuple(self, config):
        base = self._base(config)
        cid = config['configurable'].get('checkpoint_id')
        if not cid:
            latest = await self.redis.zrange(base + ':index', 0, 0, desc=True)
            if not latest:
                return None
            cid = latest[0].decode()
        raw = await self.redis.get(base + ':' + cid)
        if raw is None:
            return None
        record = self._unpack(raw)
        writes = await self.redis.hgetall(base + ':' + cid + ':writes')
        pending = [self._unpack(v) for _, v in sorted(writes.items())]
        c = {**config['configurable'], 'checkpoint_id': cid}
        parent = {**c, 'checkpoint_id': record['parent']} if record['parent'] else None
        return CheckpointTuple(config={'configurable': c}, checkpoint=record['checkpoint'],
            metadata=record['metadata'], parent_config={'configurable': parent} if parent else None,
            pending_writes=[(v['task_id'], v['channel'], v['value']) for v in pending])

    async def aput_writes(self, config, writes, task_id, task_path=''):
        key = self._base(config) + ':' + config['configurable']['checkpoint_id'] + ':writes'
        async with self.redis.pipeline(transaction=True) as pipe:
            for index, (channel, value) in enumerate(writes):
                position = WRITES_IDX_MAP.get(channel, index)
                field = f'{task_path}:{task_id}:{position}'
                packed = self._pack({'task_id': task_id, 'channel': channel, 'value': value})
                if position < 0:
                    pipe.hset(key, field, packed)
                else:
                    pipe.hsetnx(key, field, packed)
            await pipe.execute()

    async def alist(self, config, *, filter=None, before=None, limit=None):
        bases = [self._base(config)] if config else [
            key.decode().removesuffix(':index') async for key in self.redis.scan_iter(self.prefix + '*:index')]
        count = 0
        for base in bases:
            for cid_raw in await self.redis.zrange(base + ':index', 0, -1, desc=True):
                cid = cid_raw.decode()
                if before and cid >= before['configurable']['checkpoint_id']:
                    continue
                parts = base.removeprefix(self.prefix).split(':')
                decode = lambda s: base64.urlsafe_b64decode(s).decode()
                cfg = {'configurable': {'thread_id': decode(parts[0]), 'checkpoint_ns': decode(parts[1]), 'checkpoint_id': cid}}
                item = await self.aget_tuple(cfg)
                if filter and any(item.metadata.get(k) != v for k, v in filter.items()):
                    continue
                if limit is not None and count >= limit:
                    return
                yield item
                count += 1
