import os

from app.settings import ROOT

os.environ.setdefault('PHOENIX_WORKING_DIR', str(ROOT / '.runtime/phoenix-client'))
from phoenix.otel import register
from openinference.instrumentation import OITracer, TraceConfig
from openinference.instrumentation.langchain import LangChainInstrumentor
from opentelemetry.sdk.trace import TracerProvider


def configure():
    if os.getenv('TRACING_ENABLED', 'true').lower() == 'true':
        provider = register(
            project_name=os.getenv('PHOENIX_PROJECT_NAME', 'preentrega7-carga5-final'),
            endpoint=os.getenv('PHOENIX_COLLECTOR_ENDPOINT', 'http://127.0.0.1:6007/v1/traces'),
            protocol='http/protobuf', batch=True,
        )
        LangChainInstrumentor().instrument(tracer_provider=provider)
    else:
        provider = TracerProvider()
    return provider, OITracer(provider.get_tracer('preentrega7'), config=TraceConfig())


provider, tracer = configure()
