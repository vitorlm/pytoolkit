# LLM Abstraction Layer

This module provides a vendor-agnostic abstraction for LLM providers,
following Ports & Adapters (Hexagonal Architecture) pattern.

## Architecture

- **Port**: `LLMClient` protocol defines the interface
- **Adapters**: Provider-specific implementations (Portkey, z.ai, OpenAI, etc.)
- **Factory**: `LLMFactory` handles provider selection via configuration
- **DTOs**: `LLMRequest`, `LLMResponse` are vendor-agnostic

## Supported Providers

### Portkey (Syngenta AI Foundry Gateway)
- **Adapter**: `PortkeyLLMAdapter`
- **Environment Variable**: `PORTKEY_API_KEY`
- **Model Format**: `provider/model-name` (e.g., `openai/gpt-4o`)
- **Docs**: https://syngenta-digital.github.io/aifoundry-portal/docs/gateway-api-calls

### z.ai
- **Adapter**: `ZAILLMAdapter`
- **Environment Variable**: `Z_AI_API_KEY` or `ZAI_API_KEY`
- **Model Format**: `glm-4.7`, `glm-4.6`, `glm-4.5`, `glm-4.5-flash`, etc.
- **SDK**: Official `zai-sdk` Python package
- **Docs**: https://docs.z.ai/guides/develop/python/introduction
- **Features**: Built-in retry logic with SDK, automatic backoff on rate limits

#### ⚠️ Rate Limiting Considerations

**Free Tier Restrictions:**
- **Concurrent Requests**: Free tier limited to **1 concurrent request** (reduced from 3)
- **Long Context Throttling**: Requests with context >8K tokens throttled to **1% of standard concurrency**
- **Model Availability**:
  - `glm-4.6`: "Limited-time Free" with very restrictive rate limits (429 errors common)
  - `glm-4.5-flash`: Completely "Free" with more generous quotas (recommended for development)
  - `glm-4.6v-flash`: Also completely "Free"

**Recommendations:**
1. **Use `glm-4.5-flash` for development** - marked as completely "Free" without time limitations
2. **Keep context under 8K tokens** - avoid aggressive throttling
3. **Implement sequential processing** - free tier allows only 1 concurrent request
4. **Consider upgrading to GLM Coding Plan** ($100/month) for better availability and fewer limits
5. **Monitor token usage** - tokens/s performance has been decreasing since early 2025

**Common Issues:**
- 429 errors on first request usually indicate: free tier daily/hourly quota exhausted, or need to wait between requests
- Cache heavily to minimize API calls
- Users report "GLM 4.7 on the Coding plan has way better availability"

**Source**: Reddit r/ZaiGLM community reports (January 2026)

### Google Gemini
- **Adapter**: `GeminiLLMAdapter`
- **Environment Variable**: `GOOGLE_API_KEY`
- **Model Format**: `gemini-3.0-pro`, `gemini-2.0-flash`, `gemini-1.5-pro`, `gemini-1.5-flash`, etc.
- **SDK**: Official `google-genai` Python package
- **Docs**: https://ai.google.dev/
- **Features**: Caching, automatic retry logic, comprehensive error handling

#### Supported Models

- **gemini-3.0-pro**: Latest high-performance model with extended reasoning capabilities
- **gemini-2.0-flash**: Fast, efficient reasoning with good quality (recommended for most tasks)
- **gemini-1.5-pro**: Extended context window (2M tokens)
- **gemini-1.5-flash**: Fast response with good quality

#### Setup

```bash
# Install SDK
pip install google-genai

# Set API key
export GOOGLE_API_KEY="your-api-key-here"
```

**Pricing**: https://ai.google.dev/pricing

### OpenAI (Coming Soon)
- **Status**: Not yet implemented
- **Environment Variable**: `OPENAI_API_KEY`

## Adding a New Provider

1. Create adapter: `src/utils/llm/your_provider_adapter.py`
2. Implement `LLMClient` protocol
3. Add provider to `LLMFactory.create_client()`
4. Update `.env.example` with provider-specific variables

## Usage

### Basic Usage

```python
from utils.llm.llm_factory import LLMFactory
from utils.llm.llm_client import LLMRequest, LLMMessage

# Create client (provider from env)
client = LLMFactory.create_client()

# Make request
request = LLMRequest(
    messages=[LLMMessage(role="user", content="Hello!")],
    model="gpt-4o-mini"
)
response = client.chat_completion(request)
print(response.content)
```

### Provider-Specific Configuration

```python
# Use specific provider
from utils.llm.llm_factory import LLMFactory

# Use z.ai
client = LLMFactory.create_client(provider="zai")

# Use Portkey
client = LLMFactory.create_client(provider="portkey")
```

### z.ai Example

```python
from utils.llm.llm_factory import LLMFactory
from utils.llm.llm_client import LLMRequest, LLMMessage

# Create z.ai client
client = LLMFactory.create_client(provider="zai")

# Recommended: Use glm-4.5-flash (completely free, fewer rate limits)
request = LLMRequest(
    messages=[
        LLMMessage(role="system", content="You are a helpful assistant."),
        LLMMessage(role="user", content="Explain relativity simply.")
    ],
    model="glm-4.5-flash",  # Recommended over glm-4.6 for free tier
    max_completion_tokens=250
)

response = client.chat_completion(request)
print(f"Model: {response.model}")
print(f"Tokens: {response.usage}")
print(f"Response: {response.content}")
```

**Note**: If using `glm-4.6` or `glm-4.7` and encountering 429 errors immediately, switch to `glm-4.5-flash` which has more generous free tier quotas.

### Gemini Example

```python
from utils.llm.llm_factory import LLMFactory
from utils.llm.llm_client import LLMRequest, LLMMessage

# Create Gemini client
client = LLMFactory.create_client(provider="gemini")

# Use gemini-2.0-flash (recommended for most tasks)
request = LLMRequest(
    messages=[
        LLMMessage(role="system", content="You are a helpful assistant."),
        LLMMessage(role="user", content="Classify this text into categories.")
    ],
    model="gemini-2.0-flash",  # Fast and efficient
    max_completion_tokens=250
)

response = client.chat_completion(request)
print(f"Model: {response.model}")
print(f"Tokens: {response.usage}")
print(f"Response: {response.content}")
```

## Configuration

### Environment Variables

Set in `.env` file or export in shell:

```bash
# Choose provider (default: portkey)
export LLM_PROVIDER=zai  # or "portkey", "gemini", "openai"

# Portkey (Syngenta AI Foundry Gateway)
export PORTKEY_API_KEY=your_portkey_key_here

# z.ai
export Z_AI_API_KEY=your_zai_key_here

# Google Gemini
export GOOGLE_API_KEY=your_google_key_here

# OpenAI (future)
export OPENAI_API_KEY=your_openai_key_here
```

## Features

### Caching
All adapters implement intelligent caching:
- Default expiration: 24 hours (1440 minutes)
- Cache key based on: model, messages, max_tokens
- Automatic cache hit detection

### Error Handling
- Custom `LLMClientError` for all provider errors
- Detailed logging with `LogManager`
- Retry logic (z.ai adapter: 2 retries with exponential backoff)

### Type Safety
- Complete type hints using Python 3.13 built-in generics
- Vendor-agnostic DTOs ensure consistent interfaces

## Testing

```bash
# Activate virtual environment
source .venv/bin/activate

# Run tests (when implemented)
pytest tests/test_llm_adapters.py -v
```
