import json
import logging
import os
import re

from openai import APIError, OpenAI


class LLMClient:
    """Client for any OpenAI-compatible LLM server (Ollama, vLLM, LM Studio, llama.cpp, OpenRouter etc)."""

    def __init__(
        self,
        logger: logging.Logger,
        model: str = 'qwen3:8b',
        model_host: str = 'http://localhost:11434/v1',
        api_key: str | None = None,
    ) -> None:
        self.logger = logger
        self.model = model

        # OpenAI-compatible servers expose the API under a /v1 path;
        # append it when missing so plain hosts keep working.
        base_url = model_host.rstrip('/')
        if not base_url.endswith('/v1'):
            base_url += '/v1'
            logger.info('Appended /v1 to model host: %s', base_url)

        self.llm_bridge = OpenAI(
            base_url=base_url,
            api_key=api_key or os.environ.get('MODEL_API_KEY', 'ollama'),
        )

        # Best-effort model check; not all servers implement GET /v1/models.
        try:
            # single attempt: this is a best-effort check, don't retry at startup
            models_list = self.llm_bridge.with_options(max_retries=0).models.list()
            available_models = [m.id for m in models_list.data]
        except APIError:
            self.logger.info(
                'Could not list models from server; assuming model %s is available',
                self.model,
            )
        else:
            if self.model not in available_models:
                self.logger.warning(
                    'Model %s not found in available models: %s',
                    self.model,
                    available_models,
                )
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

        completion = self.llm_bridge.chat.completions.create(
            model=self.model,
            messages=[{'role': 'user', 'content': llm_prompt}],
        )
        llm_response = completion.choices[0].message.content or ''

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
            ValueError: If the response is empty, no JSON is found, or JSON parsing fails.
        """

        if not raw_output or not raw_output.strip():
            self.logger.error('LLM returned an empty response')
            msg = 'LLM returned an empty response'
            raise ValueError(msg)

        pattern = r'```json\s*(.*?)\s*```'
        match = re.search(pattern, raw_output, re.DOTALL)

        if not match:
            self.logger.error('No JSON code block found in model output')
            msg = 'No JSON code block found in model output'
            raise ValueError(msg)

        json_str = match.group(1)

        try:
            parsed = json.loads(json_str)
            if schema_class:
                parsed = schema_class(**parsed)
        except json.JSONDecodeError as e:
            msg = f'Failed to parse JSON: {e}\nExtracted: {json_str}'
            raise ValueError(msg) from e
        return raw_output, parsed
