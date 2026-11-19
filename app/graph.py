import json

from langchain_core.messages import AIMessage
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, Field, ValidationError

from app.agents.analyst_agent import CalculationPlan, make_analyst
from app.agents.research_agent import make_researcher
from app.model import build_model, structured
from app.state import AgentName, SharedState
from app.hitl import human_approval, make_publisher

MAX_STEPS = 6


class Decision(BaseModel):
    next_agent: AgentName
    instruction: str = Field(min_length=1, max_length=140)
    reason: str = Field(min_length=1, max_length=120)


def validate_results(state):
    documents = state.get('research', {}).get('documents', [])
    sources = {doc.get('source') for doc in documents if doc.get('text')}
    if not {'chunking.md', 'dimensionamiento.md'}.issubset(sources):
        return 'researcher', 'Falta evidencia: consultá chunking y dimensionamiento con sus fuentes.'
    result = state.get('analysis', {})
    try:
        values = CalculationPlan.model_validate(result)
    except ValidationError:
        return 'analyst', 'Falta un cálculo válido con los tres parámetros del pedido.'
    stride = values.chunk_size - values.overlap
    expected = 1 + max(0, (values.total_tokens - values.chunk_size + stride - 1) // stride)
    if result.get('chunks') != expected or result.get('coverage', 0) < values.total_tokens:
        return 'analyst', 'El cálculo no cubre el documento o contradice la fórmula; recalculá.'
    if result.get('source') not in sources:
        return 'analyst', 'El cálculo debe referenciar una fuente recuperada por investigación.'
    return None, ''


def synthesize(state):
    r = state['analysis']
    return (f"El chunking divide el documento y el solapamiento conserva contexto [chunking.md]. "
            f"Para {r['total_tokens']} tokens, ventanas de {r['chunk_size']} y solapamiento "
            f"de {r['overlap']}, el avance es {r['stride']} tokens. "
            f"Se necesitan {r['chunks']} fragmentos: "
            f"1 + ceil(max(0, {r['total_tokens']} - {r['chunk_size']}) / {r['stride']}) "
            f"[dimensionamiento.md]. Es una estimación de ventanas fijas; "
            f"un splitter que respeta párrafos puede producir otra cantidad.")


def build_graph(model=None, checkpointer=None, redis=None, prefix='pre7'):
    model = model or build_model()

    async def supervisor(state: SharedState):
        steps = state.get('steps', 0) + 1
        missing, feedback = validate_results(state)
        if missing is None:
            answer = synthesize(state)
            return {'next_agent': 'finish', 'steps': steps, 'task_completed': True,
                    'final_answer': answer, 'feedback': '',
                    'messages': [AIMessage(name='supervisor', content=answer)],
                    'decisions': [{'agent': 'finish', 'reason': 'Evidencia y cálculo validados por la rúbrica',
                                   'validation': 'Fuentes, fórmula y cobertura consistentes'}]}
        if steps > MAX_STEPS:
            return {'next_agent': 'finish', 'task_completed': False, 'steps': steps,
                    'final_answer': 'No se obtuvo una solución validada dentro del límite de pasos.',
                    'decisions': [{'agent': 'finish', 'reason': 'Límite de pasos alcanzado'}]}
        decision = await structured(model, Decision,
            'Sos el supervisor de un equipo jerárquico. Elegí dinámicamente researcher, analyst o finish. '
            'Investigador: consultar notas con fuentes. Analista: extraer parámetros y calcular. '
            'Rúbrica: debe haber evidencia técnica citada, cálculo con parámetros positivos, '
            'solapamiento menor que tamaño, cobertura suficiente y consistencia con la fórmula. '
            'Si falta evidencia, delegá al investigador; si falta el cálculo o es inconsistente, '
            'al analista. Podés pedir refinamientos. Solo finish cuando ambos sean suficientes. '
            'Tu instrucción debe ser específica para el especialista. No resuelvas vos sus tareas.',
            json.dumps({'request': state['request'], 'research': state.get('research', {}),
                        'analysis': state.get('analysis', {}), 'validation_feedback': feedback,
                        'previous_feedback': state.get('feedback', ''), 'step': steps}, ensure_ascii=False))
        target = decision.next_agent
        # Una finalización prematura se convierte en una solicitud de refinamiento.
        if target == 'finish' and missing:
            target = missing
        update = {'next_agent': target, 'instruction': decision.instruction + (' ' + feedback if missing else ''),
                  'steps': steps, 'feedback': feedback,
                  'decisions': [{'agent': target, 'reason': decision.reason,
                                 'validation': feedback or 'Resultados consistentes'}]}
        if target == 'finish':
            answer = synthesize(state)
            update.update(task_completed=True, final_answer=answer,
                          messages=[AIMessage(name='supervisor', content=answer)])
        return update

    def safe_specialist(node):
        async def wrapped(state):
            try:
                return await node(state)
            except (ValidationError, ValueError) as exc:
                return {'feedback': f'Corregir salida del especialista: {type(exc).__name__}',
                        'contributions': [{'agent': state['next_agent'], 'tool': 'validation',
                                           'arguments': {}, 'result': {'error': type(exc).__name__}}]}
        return wrapped

    def route(state: SharedState):
        if state['next_agent'] == 'finish' and state.get('task_completed') and state.get('action') == 'publish_report':
            return 'approval'
        return state['next_agent']

    graph = StateGraph(SharedState)
    graph.add_node('Supervisor', supervisor)
    graph.add_node('researcher', safe_specialist(make_researcher(model)))
    graph.add_node('analyst', safe_specialist(make_analyst(model)))
    graph.add_node('HumanApproval', human_approval)
    graph.add_node('PublishReport', make_publisher(redis, prefix))
    graph.add_edge(START, 'Supervisor')
    graph.add_conditional_edges('Supervisor', route,
                               {'researcher': 'researcher', 'analyst': 'analyst', 'finish': END, 'approval': 'HumanApproval'})
    graph.add_edge('researcher', 'Supervisor')
    graph.add_edge('analyst', 'Supervisor')
    graph.add_conditional_edges('HumanApproval', lambda state: bool(state.get('approved')),
                                {True: 'PublishReport', False: END})
    graph.add_edge('PublishReport', END)
    return graph.compile(checkpointer=checkpointer)
