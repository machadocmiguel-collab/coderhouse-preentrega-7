import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')
REDIS_URL = os.getenv('REDIS_URL', 'redis://127.0.0.1:6387/0')
API_TOKEN = os.getenv('API_TOKEN', 'local-demo')
PREFIX = os.getenv('REDIS_PREFIX', 'pre7')
WORKERS = int(os.getenv('WORKERS', '1'))
JOB_TIMEOUT = int(os.getenv('JOB_TIMEOUT', '900'))
