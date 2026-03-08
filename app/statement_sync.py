import logging
from datetime import datetime
from typing import Any

from docling.document_converter import DocumentConverter

from app.utils.api_client import APIClient
from app.utils.fs_client import FSClient
from app.utils.llm_client import LLMClient


def get_transactions(
    logger: logging.Logger,
    api_handler: APIClient,
    llm_handler: LLMClient,
    prompt: str,
    file_name: str,
) -> list[list[Any]]:

    transactions: list[list[Any]] = []

    logger.info(file_name)
    converter = DocumentConverter()
    result = converter.convert(file_name)

    llm_reasoning, llm_prediction = llm_handler.get_llm_response(
        llm_prompt=prompt + '\n\n' + result.document.export_to_markdown()
    )

    if llm_prediction:
        for prediction in llm_prediction:
            account_number = prediction.get('account_number')
            if account_number is None:
                logger.warning(
                    'No account number found in prediction, skipping transaction'
                )

            account_id = api_handler.get_account_id(account_number)
            if account_id is None:
                logger.warning(
                    'No account ID found for account number %s, skipping transaction',
                    account_number,
                )

            transaction_date = prediction.get('transaction_date')
            if transaction_date is None:
                logger.warning('No transaction_date date found, skipping transaction')

            cycle_id = api_handler.get_cycle_id_for_date(transaction_date)
            if cycle_id is None:
                logger.warning(
                    'No cycle ID found for date %s, skipping transaction',
                    transaction_date,
                )

            transactions.append([llm_reasoning, prediction, account_id, cycle_id])

    return transactions


def post_transaction(
    logger: logging.Logger, api_handler: APIClient, transaction: list[Any]
) -> None:
    account_id = transaction[2]
    if account_id is None:
        error_msg = 'account_id cannot be None'
        raise ValueError(error_msg)

    api_handler.save_transaction(
        e_mail=None,
        load_by='agent',
        llm_reasoning=transaction[0],
        llm_prediction=transaction[1],
        account_id=account_id,
        cycle_id=transaction[3],
    )
    logger.info('Transaction stored to DB')

    return


def statement_sync(
    logger: logging.Logger,
    statement_file: str | None,
    statement_folder: str | None,
    model_host: str,
    model: str,
    api_host: str,
    api_headers: dict[str, str] | None,
    prompt_file: str,
) -> None:

    api_handler = APIClient(logger, api_host, headers=api_headers)
    llm_handler = LLMClient(logger=logger, model_host=model_host, model=model)

    try:
        with open(prompt_file) as f:
            prompt = f.read()
    except OSError:
        prompt = ''

    files_to_process: dict = {}

    if statement_file is not None:
        files_to_process[statement_file] = None
    elif statement_folder is not None:
        file_system_client = FSClient(statement_folder)
        file_list = file_system_client.get_files_by_created_date()

        last_processed_file_ts = api_handler.get_latest_checkpoint(statement_folder)

        for file, created_at in file_list.items():
            created_at_ts = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')  # noqa: DTZ007

            if (last_processed_file_ts is None) or (
                created_at_ts
                > datetime.strptime(  # noqa: DTZ007
                    last_processed_file_ts, '%Y-%m-%d %H:%M:%S'
                )
            ):
                files_to_process[str(file)] = created_at_ts
    else:
        logger.error('None of statement_file or statement_folder are provided')

    if not files_to_process:
        logger.info('No files to process')
        return

    for file_name, create_ts in files_to_process.items():
        transactions = get_transactions(
            logger, api_handler, llm_handler, prompt, file_name
        )

        if transactions:
            for transaction in transactions:
                post_transaction(logger, api_handler, transaction)
        else:
            logger.info('No transactions to process')

        if statement_folder is not None:
            ckpt = datetime.strftime(create_ts, '%Y-%m-%d %H:%M:%S')
            api_handler.set_latest_checkpoint(statement_folder, ckpt)
            logger.info('Checkpoint saved')
