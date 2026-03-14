import logging
from typing import Any, cast

import requests


class APIClient:
    def __init__(
        self,
        logger: logging.Logger,
        base_url: str = 'http://127.0.0.1:8000',
        session: requests.Session | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.logger = logger
        self.base = base_url.rstrip('/')
        self.s = session or requests.Session()
        if headers:
            self.s.headers.update(headers)

    def get_last_seen_uid(self, folder: str) -> int | None:
        r = self.s.get(f'{self.base}/email_checkpoints/{folder}')
        if r.status_code == 200:
            response_json = cast('dict[str, Any]', r.json())
            return response_json.get('last_seen_uid')
        return None

    def set_last_seen_uid(self, folder: str, uid: int) -> dict[str, Any]:
        r = self.s.put(
            f'{self.base}/email_checkpoints/{folder}', json={'last_seen_uid': uid}
        )
        r.raise_for_status()
        return cast('dict[str, Any]', r.json())

    def get_latest_checkpoint(self, identifier: str) -> str | None:
        r = self.s.get(f'{self.base}/checkpoints/{identifier}')
        if r.status_code == 200:
            response_json = cast('dict[str, Any]', r.json())
            return response_json.get('checkpoint')
        return None

    def set_latest_checkpoint(self, identifier: str, checkpoint: str) -> dict[str, Any]:
        r = self.s.put(
            f'{self.base}/checkpoints/{identifier}', json={'load_by': 'transactsync-backend', 'checkpoint': checkpoint}
        )
        r.raise_for_status()
        return cast('dict[str, Any]', r.json())

    def get_file_id_by_name(self, file_name: str) -> int | None:
        r = self.s.get(f'{self.base}/files/path-name//{file_name}')
        if r.status_code == 200:
            response_json = cast('dict[str, Any]', r.json())
            return response_json.get('file_id')
        return None

    def set_file_id_by_name(
        self, file_name: str, file_created_at: str
    ) -> dict[str, Any]:
        payload = {
            'load_by': 'transactsync-backend',
            'file_name': file_name.rsplit('/', 1)[0],
            'file_path': file_name.rsplit('/', 1)[1],
            'file_created_at': file_created_at,
        }
        r = self.s.post(f'{self.base}/files', json=payload)
        r.raise_for_status()
        return cast('dict[str, Any]', r.json())

    def get_email_id_by_email(self, email: dict[str, str], folder: str) -> int | None:
        r = self.s.get(
            f'{self.base}/emails/uid/{email["uid"]}?folder={folder}&from_address={email["from_address"]}&to_address={email["to_address"]}&email_date={email["email_date"]}'
        )
        if r.status_code == 200:
            response_json = cast('dict[str, Any]', r.json())
            return response_json.get('email_id')
        return None

    def set_email_id_by_email(
        self, email: dict[str, str], folder: str
    ) -> dict[str, Any]:
        payload = {
            'load_by': 'transactsync-backend',
            'email_uid': email['uid'],
            'folder': folder,
            'from_address': email['from_address'],
            'to_address': email['to_address'],
            'email_date': email['email_date'],
        }
        r = self.s.post(f'{self.base}/emails', json=payload)
        r.raise_for_status()
        return cast('dict[str, Any]', r.json())

    def get_account_id(self, account_number: str) -> int | None:
        """Resolve account_id by account_number via API.

        Expects an endpoint that accepts a query param `account_number` and
        returns JSON with `account_id`.
        """
        r = self.s.get(
            f'{self.base}/accounts/by-number', params={'account_number': account_number}
        )
        if r.status_code == 200:
            response_json = cast('dict[str, Any]', r.json())
            return response_json.get('account_id')
        return None

    def get_cycle_id_for_date(self, transaction_date: str) -> int | None:
        r = self.s.get(
            f'{self.base}/cycles/for-date',
            params={'transaction_date': transaction_date},
        )
        if r.status_code == 200:
            response_json = cast('dict[str, Any]', r.json())
            return response_json.get('cycle_id')
        return None

    def save_transaction(
        self,
        load_by: str,
        transaction_date: str,
        llm_reasoning: str,
        llm_prediction: dict[str, Any],
        account_id: int,
        cycle_id: int | None = None,
        email_id: int | None = None,
        file_id: int | None = None,
    ) -> dict[str, Any]:

        payload = {
            'load_by': load_by,
            'transaction_date': transaction_date,
            'transaction_type': llm_prediction.get('transaction_type'),
            'transaction_amount': llm_prediction.get('transaction_amount'),
            'merchant': llm_prediction.get('merchant'),
            'account_id': account_id,
            'cycle_id': cycle_id,
            'email_id': email_id,
            'file_id': file_id,
            'llm_reasoning': llm_reasoning,
            'comment': llm_prediction.get('comment'),
            'is_budgeted': False,
            'is_deleted': False,
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        r = self.s.post(f'{self.base}/transactions', json=payload)
        r.raise_for_status()
        return cast('dict[str, Any]', r.json())
