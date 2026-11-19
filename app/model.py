import os
from pathlib import Path

from dotenv import load_dotenv
from langchain_ollama import ChatOllama
from app.observability import tracer

load_dotenv(Path(__file__).with_name('.env'))


def build_model():
    return ChatOllama(
        model=os.getenv('OLLAMA_MODEL', 'llama3.1:latest'),
        base_url=os.getenv('OLLAMA_BASE_URL', 'http://127.0.0.1:11434'),
        temperature=0, num_predict=160, num_ctx=2048,
        client_kwargs={'timeout': 120}, async_client_kwargs={'timeout': 120},
    )


@tracer.chain(name='llm_structured')
async def structured(model, schema, system, payload):
    runnable = model.with_structured_output(schema, method='json_schema')
    return await runnable.ainvoke([('system', system + ' Respondé de forma mínima, con frases cortas.'), ('human', payload)])
