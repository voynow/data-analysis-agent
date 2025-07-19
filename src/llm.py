import asyncio
from typing import Optional, Type

from dotenv import load_dotenv
from openai import AsyncOpenAI
from openai.types.chat.chat_completion_message import ChatCompletionMessage
from pydantic import BaseModel

load_dotenv()
client = AsyncOpenAI()


async def get_completion(
    message: str, model: Optional[str] = "gpt-4.1"
) -> ChatCompletionMessage:
    """
    LLM completion with raw string response

    :param message: The message to send to the LLM.
    :param model: The model to use for the completion.
    :return: The raw string response from the LLM.
    """
    messages = [{"role": "user", "content": message}]
    response = await client.chat.completions.create(model=model, messages=messages)
    return response.choices[0].message.content


async def get_completion_structured(
    message: str,
    response_model: Type[BaseModel],
    model: str = "gpt-4.1",
    max_completion_tokens: int = 1024,
    max_retries: int = 2,
    retry_delay: float = 1.0,
) -> BaseModel:
    """
    Get a structured completions backed by pydantic validation with retry logic for length errors

    :param message: The message to send to the LLM.
    :param response_model: The Pydantic model to parse the response into.
    :param function_name: The name of the function calling this for logging.
    :param model: The model to use for the completion.
    :param max_completion_tokens: Maximum tokens for the completion response.
    :param max_retries: Maximum number of retry attempts.
    :param retry_delay: Base delay between retries (exponential backoff).
    :return: The parsed Pydantic model instance.
    :raises: LengthFinishReasonError if all retries fail due to length limits.
    """
    for attempt in range(max_retries + 1):
        try:
            messages = [{"role": "user", "content": message}]
            response = await client.beta.chat.completions.parse(
                model=model,
                messages=messages,
                response_format=response_model,
                max_completion_tokens=max_completion_tokens,
            )
            return response.choices[0].message.parsed

        except Exception as e:
            if attempt < max_retries:
                await asyncio.sleep(retry_delay)
                continue
            else:
                raise e
