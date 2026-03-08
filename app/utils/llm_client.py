import json
import logging
import re

from ollama import Client


class LLMClient:
    def __init__(
        self,
        logger: logging.Logger,
        model: str = 'qwen3:8b',
        model_host: str = 'http://localhost:11434',
    ) -> None:
        self.logger = logger
        self.model = model
        self.model_host = model_host
        self.llm_bridge = Client(host=self.model_host)
        if self.model not in [m.model for m in self.llm_bridge.list().models]:
            self.logger.info(
                'Model not found in available models. Pulling model: %s', self.model
            )
            self.llm_bridge.pull(self.model)
        self.logger.info('Using model: %s', self.model)

    def get_transaction(
        self, e_mail: dict, llm_prompt: str | None = None
    ) -> tuple[str, dict]:
        """
        Extract transaction details from an email using a language model.

        Args:
            e_mail (dict): A dictionary containing the email's subject, date, sender, recipient, and body.
            llm_prompt (Optional[str]): An optional custom prompt to use with the language model.

        Returns:
            Tuple[str, dict]: A tuple containing the reasoning text and the parsed JSON object.

        Raises:
            ValueError: If no JSON is found or JSON parsing fails.
        """

        if llm_prompt is None:
            self.logger.info('Using default prompt')
            # These variables should be defined elsewhere in the codebase
            # For now, we'll define them here to avoid undefined name errors

            llm_prompt = """
            You are given an email body and you must extract transaction information when the email represents a transaction.

            Determine if the email represents a transaction. If so, extract and return JSON with these keys (no extras):
            - account_number (string - include only the last 4 digits)
            - transaction_amount (float)
            - merchant (string, if available)
            - transaction_type (string: 'debit'|'credit')
            - transaction_flag (boolean)

            If the email is not a transaction, return:
            {"transaction_flag": false}

            Do not include explanatory text in your response — only return valid JSON matching the schema above.
            """
        llm_prompt = (
            llm_prompt
            + f"""
            \n from_address: {e_mail['from_address']}
            \n date: {e_mail['email_date']}
            \n subject: {e_mail['subject']}
            \n body: \n{e_mail['body'].strip()}
            """.strip()
        )

        llm_response = self.llm_bridge.generate(
            model=self.model, prompt=llm_prompt
        ).response

        llm_reasoning, llm_prediction = self.parse_model_output(llm_response)

        return llm_reasoning, llm_prediction

    @staticmethod
    def parse_model_output(
        raw_output: str, schema_class: type | None = None
    ) -> tuple[str, dict]:
        """
        Parse the raw output from a language model to extract reasoning text and structured data.

        Args:
            raw_output (str): The raw output string from the language model.
            schema_class (Optional[type]): A Pydantic class to validate/parse the extracted JSON.

        Returns:
            Tuple[str, dict]: A tuple containing the reasoning text and the parsed JSON object.

        Raises:
            ValueError: If no JSON is found or JSON parsing fails.
        """
        # Match from the first '{' to the last '}' (greedy) — fallback if not recursive
        json_match = re.search(r'\{(?:.|\n)*?\}', raw_output)

        if not json_match:
            msg = 'No JSON object found in model output.'
            raise ValueError(msg)

        json_str = json_match.group(0)
        reasoning_text = raw_output[: json_match.start()].strip()

        try:
            parsed = json.loads(json_str)
            if schema_class:
                parsed = schema_class(**parsed)
        except json.JSONDecodeError as e:
            msg = f'Failed to parse JSON: {e}\nExtracted: {json_str}'
            raise ValueError(msg) from e
        return reasoning_text, parsed