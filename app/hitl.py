import json

from langgraph.types import interrupt
from langchain_core.tools import tool
from pydantic import BaseModel, Field


async def human_approval(state):
    decision = interrupt({'question': '¿Autorizás publicar el informe en el almacenamiento de reportes?',
                          'action': 'publish_report', 'report_preview': state['final_answer']})
    approved = decision is True
    if not approved:
        return {'approved': False, 'task_completed': False, 'final_answer': 'Publicación rechazada; no se guardó ningún reporte.'}
    return {'approved': True}


class ReportInput(BaseModel):
    job_id: str = Field(min_length=1)
    report: str = Field(min_length=1)


def make_publisher(redis, prefix):
    @tool(args_schema=ReportInput)
    async def publish_report(job_id: str, report: str) -> str:
        """Publica un informe aprobado en el registro persistente de reportes de Redis."""
        await redis.set(f'{prefix}:report:{job_id}', json.dumps({'report': report}, ensure_ascii=False))
        return 'Informe publicado'

    async def publisher(state):
        if not state.get('approved'):
            raise PermissionError('La publicación requiere aprobación externa')
        await publish_report.ainvoke({'job_id': state['job_id'], 'report': state['final_answer']})
        return {'published': True}
    return publisher
