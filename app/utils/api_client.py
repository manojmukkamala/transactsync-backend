from typing import Any, cast

import requests


class APIClient:
    def __init__(
        self,
        base_url: str = 'http://127.0.0.1:8000',
        session: requests.Session | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
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

    def get_account_ids_dict(self) -> dict[tuple, int]:
        r = self.s.get(f'{self.base}/accounts')
        if r.status_code == 200:
            accounts = cast('dict[str, Any]', r.json())
            result: dict[tuple, int] = {}
            for a in accounts:
                # accounts is a list of dictionaries, so we need to properly type it
                account_dict = cast('dict[str, Any]', a)
                key = (
                    account_dict.get('financial_institution'),
                    account_dict.get('account_number'),
                )
                if key[0] is not None and key[1] is not None:
                    result[key] = account_dict.get(
                        'account_id', 0
                    )  # Default to 0 if not found
            return result
        return {}

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
        e_mail: dict[str, Any],
        load_by: str,
        llm_reasoning: str,
        llm_prediction: dict[str, Any],
        account_id: int,
        cycle_id: int | None = None,
    ) -> dict[str, Any]:
        payload = {
            'load_by': load_by,
            'transaction_date': e_mail.get('email_date'),
            'transaction_amount': llm_prediction.get('transaction_amount'),
            'merchant': llm_prediction.get('merchant'),
            'account_id': account_id,
            'from_address': e_mail.get('from_address'),
            'to_address': e_mail.get('to_address'),
            'email_uid': e_mail.get('uid'),
            'email_date': e_mail.get('email_date'),
            'transaction_type': llm_prediction.get('transaction_type'),
            'llm_reasoning': llm_reasoning,
            'comment': llm_prediction.get('comment'),
            'cycle_id': cycle_id,
            'is_deleted': False,
            'is_budgeted': False
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        r = self.s.post(f'{self.base}/transactions', json=payload)
        r.raise_for_status()
        return cast('dict[str, Any]', r.json())