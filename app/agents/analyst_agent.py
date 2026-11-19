import json

from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from pydantic import BaseModel, Field, model_validator

from app.model import structured
from app.state import SharedState


class CalculationPlan(BaseModel):
    total_tokens: int = Field(gt=0, description='Cantidad de tokens indicada por el usuario')
    chunk_size: int = Field(gt=0, description='Tamaño máximo indicado por el usuario')
    overlap: int = Field(ge=0, description='Solapamiento indicado por el usuario; puede ser cero')

    @model_validator(mode='after')
    def valid_overlap(self):
        if self.overlap >= self.chunk_size:
            raise ValueError('El solapamiento debe ser menor que el tamaño del fragmento')
        return self


@tool(args_schema=CalculationPlan)
async def calculate_chunks(total_tokens: int, chunk_size: int, overlap: int) -> dict:
    """Calcula exactamente la cantidad mínima de ventanas fijas necesarias para cubrir tokens."""
    stride = chunk_size - overlap
    chunks = 1 + max(0, (total_tokens - chunk_size + stride - 1) // stride)
    return {'total_tokens': total_tokens, 'chunk_size': chunk_size, 'overlap': overlap,
            'stride': stride, 'chunks': chunks,
            'coverage': chunk_size + (chunks - 1) * stride,
            'source': 'dimensionamiento.md'}


def make_analyst(model):
    async def analyst(state: SharedState):
        plan = await structured(model, CalculationPlan,
            'Sos el analista de dimensionamiento. Extraé total_tokens, chunk_size y overlap '
            'del pedido del usuario para llamar calculate_chunks. No calcules mentalmente '
            'ni uses los números de los ejemplos de las fuentes como entrada. '
            'Respetá los valores pedidos; usá la investigación solo para comprender la fórmula.',
            json.dumps({'request': state['request'], 'instruction': state['instruction'],
                        'research': state.get('research', {}), 'feedback': state.get('feedback', '')},
                       ensure_ascii=False))
        result = await calculate_chunks.ainvoke(plan.model_dump())
        return {'analysis': result, 'feedback': '',
                'messages': [AIMessage(name='analyst', content=json.dumps(result, ensure_ascii=False))],
                'contributions': [{'agent': 'analyst', 'tool': 'calculate_chunks',
                                   'arguments': plan.model_dump(), 'result': result}]}
    return analyst
