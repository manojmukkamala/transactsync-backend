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

            transactions.append(
                [llm_reasoning, prediction, account_id, cycle_id, transaction_date]
            )

    return transactions


def post_transaction(
    logger: logging.Logger,
    api_handler: APIClient,
    file_id: int | None,
    transaction: list[Any],
) -> None:
    account_id = transaction[2]
    if account_id is None:
        error_msg = 'account_id cannot be None'
        raise ValueError(error_msg)

    api_handler.save_transaction(
        load_by='agent',
        transaction_date=transaction[4],
        llm_reasoning=transaction[0],
        llm_prediction=transaction[1],
        account_id=account_id,
        cycle_id=transaction[3],
        file_id=file_id,
    )
    logger.info('Transaction stored to DB')

    return


def get_files_to_process(
    logger: logging.Logger,
    api_handler: APIClient,
    statement_file: str | None,
    statement_folder: str | None,
) -> dict:

    files_to_process: dict = {}

    if statement_file is not None:
        files_to_process[statement_file] = FSClient.get_file_created_date(
            statement_file
        )
    elif statement_folder is not None:
        file_system_client = FSClient(statement_folder)
        file_list = file_system_client.get_files_by_created_date()
        logger.info(file_list)

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

    return files_to_process


def get_file_id(api_handler: APIClient, file_name: str, file_created_at: str) -> int:
    try:
        file_id = api_handler.get_file_id_by_name(file_name)
        if file_id is None:
            response = api_handler.set_file_id_by_name(file_name, file_created_at)
            file_id = response['file_id']
    except KeyError as kerr:
        k = 'file_id'
        raise KeyError(k) from kerr
    except Exception:
        raise
    return file_id


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
        prompt = ''  # improve error handling

    files_to_process = get_files_to_process(
        logger, api_handler, statement_file, statement_folder
    )

    if not files_to_process:
        logger.info('No files to process')
        return

    for file_name, create_ts in files_to_process.items():
        ckpt = datetime.strftime(create_ts, '%Y-%m-%d %H:%M:%S')

        transactions = get_transactions(
            logger, api_handler, llm_handler, prompt, file_name
        )

        if transactions:
            file_id = get_file_id(api_handler, file_name, ckpt)
            for transaction in transactions:
                post_transaction(logger, api_handler, file_id, transaction)
        else:
            logger.info('No transactions to process')

        if statement_file is not None:
            api_handler.set_latest_checkpoint(statement_file, ckpt)
            logger.info('Checkpoint saved')
        elif statement_folder is not None:
            api_handler.set_latest_checkpoint(statement_folder, ckpt)
            logger.info('Checkpoint saved')
        else:
            logger.info('Skipping Checkpoint')
