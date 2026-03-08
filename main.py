import argparse
import json
import logging
import os

from dotenv import load_dotenv

from app.email_sync import email_sync
from app.statement_sync import statement_sync

if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=(argparse.RawDescriptionHelpFormatter)
    )
    parser.add_argument(
        '--source',
        help='Data Source to use',
        default=os.environ.get('SOURCE', 'email'),
        required=True,
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    logger = logging.getLogger(__name__)

    # Parse API headers if provided
    api_headers = None
    api_headers_env = os.environ.get('API_HEADERS')
    if api_headers_env:
        try:
            api_headers = json.loads(api_headers_env)
        except json.JSONDecodeError as e:
            parser.error(f'Invalid JSON in API_HEADERS: {e}')

    if args.source == 'email':
        email_sync(
            logger=logger,
            email_host=os.environ.get('EMAIL_HOST', '127.0.0.1'),
            email_port=int(os.environ.get('EMAIL_PORT', '143')),
            username=os.environ.get('EMAIL_USERNAME', 'admin'),
            password=os.environ.get('EMAIL_PASSWORD', 'admin'),
            folder=os.environ.get('EMAIL_FOLDER', 'INBOX'),
            model_host=os.environ.get('MODEL_HOST', 'http://localhost:11434'),
            model=os.environ.get('MODEL_NAME', 'qwen3:8b'),
            api_host=os.environ.get('API_HOST', 'http://127.0.0.1:8000'),
            api_headers=api_headers,
            transaction_rules=os.environ.get(
                'TRANSACTION_RULES', '/rules/transaction_rules.yaml'
            ),
            prompt_file=os.environ.get('PROMPT_FILE', '/rules/prompt.txt'),
        )
    elif args.source == 'statement':
        statement_sync(
            logger=logger,
            statement_file=os.environ.get('STATEMENT_FILE', None),
            statement_folder=os.environ.get('STATEMENT_FOLDER', '/data/statements'),
            model_host=os.environ.get('MODEL_HOST', 'http://localhost:11434'),
            model=os.environ.get('MODEL_NAME', 'qwen3:8b'),
            api_host=os.environ.get('API_HOST', 'http://127.0.0.1:8000'),
            api_headers=api_headers,
            prompt_file=os.environ.get('PROMPT_FILE', '/rules/prompt.txt'),
        )
