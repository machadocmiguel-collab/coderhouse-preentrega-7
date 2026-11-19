import operator
from typing import Annotated, Literal, TypedDict

from langgraph.graph import MessagesState

AgentName = Literal['researcher', 'analyst', 'finish']


class Contribution(TypedDict):
    agent: str
    tool: str
    arguments: dict
    result: dict


class SharedState(MessagesState):
    job_id: str
    action: str
    approved: bool
    published: bool
    request: str
    next_agent: AgentName
    instruction: str
    research: dict
    analysis: dict
    contributions: Annotated[list[Contribution], operator.add]
    decisions: Annotated[list[dict], operator.add]
    steps: int
    feedback: str
    task_completed: bool
    final_answer: str
