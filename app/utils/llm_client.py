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

    def get_llm_response(self, llm_prompt: str) -> tuple[str, dict]:
        """
        Extract transaction details from an email using a language model.

        Args:
            llm_prompt str: An optional custom prompt to use with the language model.

        Returns:
            Tuple[str, dict]: A tuple containing the reasoning text and the parsed JSON object.

        Raises:
            ValueError: If no JSON is found or JSON parsing fails.
        """

        llm_response = self.llm_bridge.generate(
            model=self.model, prompt=llm_prompt
        ).response

        llm_reasoning, llm_prediction = self.parse_model_output(llm_response)

        return llm_reasoning, llm_prediction

    def parse_model_output(
        self, raw_output: str, schema_class: type | None = None
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

        pattern = r'```json\s*(.*?)\s*```'
        match = re.search(pattern, raw_output, re.DOTALL)

        if not match:
            self.logger.error('No JSON code block found in markdown')
            raise ValueError

        json_str = match.group(1)

        try:
            parsed = json.loads(json_str)
            if schema_class:
                parsed = schema_class(**parsed)
        except json.JSONDecodeError as e:
            msg = f'Failed to parse JSON: {e}\nExtracted: {json_str}'
            raise ValueError(msg) from e
        return raw_output, parsed
