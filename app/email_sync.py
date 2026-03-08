import argparse
import json
import logging
import os
from email.utils import parseaddr

from app.utils.api_client import APIClient
from app.utils.email_client import EmailClient
from app.utils.llm_client import LLMClient
from app.utils.rule_parser import load_rules, match_email_to_rule


def prompt_builder(account_numbers: list[str], prompt_file: str) -> str:
    """
    Build a dynamic prompt for a single email based on the account numbers.

    This will read the prompt template and inject only the account number filter,
    clearing any from/subject filters since those are applied in code before calling the LLM.
    """
    try:
        with open(prompt_file) as f:
            tpl = f.read()
    except OSError:
        tpl = ''

    acct_filter = '`' + ', '.join(account_numbers) + '`' if account_numbers else '``'
    tpl = tpl.replace('{cc_account_number_filter}', acct_filter)
    tpl = tpl.replace('{ba_account_number_filter}', acct_filter)
    return tpl


def _initialize_components(
    email_host: str,
    email_port: int,
    username: str,
    password: str,
    folder: str,
    model_host: str,
    model: str,
    api_host: str,
    api_headers: dict[str, str] | None = None,
) -> tuple[APIClient, EmailClient, LLMClient]:
    """Initialize the API client, email handler, and LLM handler."""
    api_handler = APIClient(logger, api_host, headers=api_headers)
    logger.info('API Client created.')

    email_handler = EmailClient(
        logger, email_host, email_port, username, password, folder
    )

    llm_handler = LLMClient(logger=logger, model_host=model_host, model=model)

    return api_handler, email_handler, llm_handler


def _process_email_loop(
    email_handler: EmailClient,
    api_handler: APIClient,
    transaction_filters: dict,
    llm_handler: LLMClient,
    prompt_file: str,
    folder: str,
) -> None:
    """Process emails one-by-one: fetch UID list, then fetch and process each UID sequentially."""
    while True:
        last_seen_uid = api_handler.get_last_seen_uid(folder)
        # ensure IMAP connection exists for UID retrieval
        email_handler.imap_bridge()
        uids = email_handler.get_email_uids(
            str(last_seen_uid) if last_seen_uid is not None else None
        )
        if not uids:
            logger.info('No new emails to process.')
            break

        # Ensure processing in ascending UID order
        try:
            sorted_uids = sorted(uids, key=int)
        except ValueError:
            sorted_uids = uids

        for uid in sorted_uids:
            _process_single_email(
                uid,
                email_handler,
                api_handler,
                transaction_filters,
                llm_handler,
                prompt_file,
                folder,
            )


def _process_single_email(
    uid: str,
    email_handler: EmailClient,
    api: APIClient,
    transaction_filters: dict,
    transaction_handler: LLMClient,
    prompt_file: str,
    folder: str,
) -> None:
    """Process a single email for transaction extraction."""
    e_mail = email_handler.get_email_by_uid(uid)
    if not e_mail:
        # still commit the uid to avoid reprocessing a failing message
        api.set_last_seen_uid(folder, int(uid))
        return

    # Match email to a rule (sender + subject)
    matched_rule = match_email_to_rule(
        e_mail.get('subject', ''),
        e_mail.get('from_address', ''),
        transaction_filters,
    )
    if not matched_rule:
        logger.info('Skipping email UID %s - no matching rule', uid)
        api.set_last_seen_uid(folder, int(uid))
        return

    account_numbers = matched_rule.get('account_numbers', [])
    llm_prompt_for_email = prompt_builder(account_numbers, prompt_file)

    llm_prompt_for_email = (
        llm_prompt_for_email
        + f"""
        \n from_address: {e_mail['from_address']}
        \n date: {e_mail['email_date']}
        \n subject: {e_mail['subject']}
        \n body: \n{e_mail['body'].strip()}
        """.strip()
    )

    llm_reasoning, llm_prediction = transaction_handler.get_llm_response(
        llm_prompt_for_email
    )
    _, e_mail['from_address'] = parseaddr(e_mail['from_address'])
    _, e_mail['to_address'] = parseaddr(e_mail['to_address'])

    if llm_prediction and llm_prediction.get('transaction_flag'):
        llm_reasoning = llm_reasoning.replace('"', '`').replace("'", '`')
        logger.info('from_address: %s', e_mail['from_address'])
        logger.info('to_address: %s', e_mail['to_address'])
        logger.info('email_uid: %s', e_mail['uid'])
        logger.info('email_date: %s', e_mail['email_date'])
        logger.info('email_subject: %s', e_mail['subject'])
        logger.info('llm_prediction: %s', llm_prediction)
        logger.info('llm_reasoning: %s', llm_reasoning)
        account_number = llm_prediction.get('account_number')
        if account_number is None:
            logger.warning(
                'No account number found in prediction, skipping transaction'
            )
            api.set_last_seen_uid(folder, int(uid))
            return
        account_id = api.get_account_id(account_number)
        if account_id is None:
            logger.warning(
                'No account ID found for account number %s, skipping transaction',
                account_number,
            )
            api.set_last_seen_uid(folder, int(uid))
            return
        email_date = e_mail.get('email_date')
        if email_date is None:
            logger.warning('No email date found, skipping transaction')
            api.set_last_seen_uid(folder, int(uid))
            return
        cycle_id = api.get_cycle_id_for_date(email_date)
        if cycle_id is None:
            logger.warning(
                'No cycle ID found for date %s, skipping transaction',
                email_date,
            )
            api.set_last_seen_uid(folder, int(uid))
            return
        api.save_transaction(
            e_mail=e_mail,
            load_by='agent',
            llm_reasoning=llm_reasoning,
            llm_prediction=llm_prediction,
            account_id=account_id,
            cycle_id=cycle_id,
        )
        logger.info('Transaction stored to DB')
    else:
        logger.info('Skipping non-transaction or invalid prediction')

    # Commit this UID so next iteration will start after it
    api.set_last_seen_uid(folder, int(uid))


def email_sync(
    logger: logging.Logger,
    email_host: str,
    email_port: int,
    username: str,
    password: str,
    folder: str,
    model_host: str,
    model: str,
    api_host: str,
    api_headers: dict[str, str] | None,
    transaction_rules: str,
    prompt_file: str,
) -> None:
    """
    Main synchronization routine for fetching emails, extracting transactions, and storing them in the database.

    - Loads transaction rules and builds the LLM prompt.
    - Fetches new emails from the specified folder since the last checkpoint (UID).
    - For each email, uses the LLM to extract transaction details and stores valid transactions in the DB.
    - Updates the checkpoint (last seen UID) in the DB for the folder.

    Args:
        email_host (str): IMAP server address.
        email_port (int): IMAP server port.
        username (str): Email account username.
        password (str): Email account password.
        folder (str): Email folder to fetch from.
        transaction_rules (str): Path to transaction rules YAML file.
        prompt_file (str): Path to prompt template file.
        model_host (str, optional): LLM model host URL. Default: "http://localhost:11434".
        model (str, optional): LLM model name. Default: "qwen3:8b".
    """

    # Load rules (new spec)
    transaction_filters = load_rules(transaction_rules)

    # Initialize components
    api_handler, email_handler, llm_handler = _initialize_components(
        email_host,
        email_port,
        username,
        password,
        folder,
        model_host,
        model,
        api_host,
        api_headers,
    )

    # Process emails one-by-one: fetch UID list, then fetch and process each UID sequentially committing the UID after each
    _process_email_loop(
        email_handler,
        api_handler,
        transaction_filters,
        llm_handler,
        prompt_file,
        folder,
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=(argparse.RawDescriptionHelpFormatter)
    )
    parser.add_argument(
        '--email_host',
        help='Email Host (IMAP Server Address)',
        default=os.environ.get('EMAIL_HOST'),
        required=False,
    )
    parser.add_argument(
        '--email_port',
        help='Email Host (IMAP Server Address)',
        default=os.environ.get('EMAIL_PORT'),
        required=False,
    )
    parser.add_argument(
        '--username',
        help='Email Account Username',
        default=os.environ.get('EMAIL_USERNAME'),
        required=False,
    )
    parser.add_argument(
        '--password',
        help='Email Account Password',
        default=os.environ.get('EMAIL_PASSWORD'),
        required=False,
    )
    parser.add_argument(
        '--folder',
        help='Folder Name',
        default=os.environ.get('EMAIL_FOLDER', 'INBOX'),
        required=False,
    )
    parser.add_argument(
        '--transaction_rules',
        help='Transaction Rules File',
        default='/examples/transaction_rules.yaml',
    )
    parser.add_argument(
        '--prompt_file', help='Prompt File', default='/examples/prompt.txt'
    )
    parser.add_argument(
        '--model_host',
        help='Model Host (default: http://localhost:11434)',
        default=os.environ.get('MODEL_HOST', 'http://localhost:11434'),
        required=False,
    )
    parser.add_argument(
        '--model',
        help='Model Name (default: qwen3:8b)',
        default=os.environ.get('MODEL_NAME', 'qwen3:8b'),
        required=False,
    )
    parser.add_argument(
        '--api_host',
        help='API Host (default: http://127.0.0.1:8000)',
        default=os.environ.get('API_HOST', 'http://127.0.0.1:8000'),
        required=False,
    )
    parser.add_argument(
        '--api_headers',
        help='API Headers as JSON string (e.g., \'{"x-api-key": "super-secret"}\')',
        default=os.environ.get('API_HEADERS'),
        required=False,
    )
    args = parser.parse_args()

    # Validate required arguments (env or CLI)
    missing = []
    if not args.email_host:
        missing.append('email_host (or EMAIL_HOST env var)')
    if not args.email_port:
        missing.append('email_port (or EMAIL_PORT env var)')
    if not args.username:
        missing.append('username (or EMAIL_USERNAME env var)')
    if not args.password:
        missing.append('password (or EMAIL_PASSWORD env var)')
    if not args.folder:
        missing.append('folder (or EMAIL_FOLDER env var)')
    if missing:
        parser.error('Missing required arguments: ' + ', '.join(missing))

    # Parse API headers if provided
    api_headers = None
    if args.api_headers:
        try:
            api_headers = json.loads(args.api_headers)
        except json.JSONDecodeError as e:
            parser.error(f'Invalid JSON in api_headers: {e}')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    logger = logging.getLogger(__name__)

    email_sync(
        logger,
        args.email_host,
        args.email_port,
        args.username,
        args.password,
        args.folder,
        args.model_host,
        args.model,
        args.api_host,
        api_headers,
        args.transaction_rules,
        args.prompt_file,
    )
