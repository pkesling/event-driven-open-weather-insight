# AI Usage in Project Development
One of the goals of this project (see README.md) was to use modern AI tools to assist in developing an event-driven
data engineering platform.  This document describes the AI tools used, how the tools were leveraged, and observations
on the experience.

## Goals
The goals for using Gen AI to assist in the development of this project were:
   - Treat AI as a coding partner during design and development
   - Leverage AI to:
     - Summarize and explain new tools, libraries, concepts, etc.
     - Compare/Contrast approaches (design patterns) to use throughout development
     - Write unit tests
     - Create the initial code for some files (usually requiring human intervention to get right)
     - Generate initial doc strings
     - Optimize code adhering to Python best practices
     - Check for PEP8 adherence
     - Review code

## Personal Background with Gen AI Assisted Software Development
I've been using Gen AI to assist with software development for many years.  Coming into this project, these were my 
general thoughts and impressions.
  - Gen AI reduced the time to learn new topics, languages, domains by more than 10X, often with a much better level
of understanding.  Gen AI became my personal tutor.
  - As such, the breadth and depth of development work (data engineering, data analysis, microservices, etc.) increased
dramatically, almost overnight.
    - New programming languages were no longer a barrier that might have previously taken weeks to learn just to get 
started.
    - Proper design and development in new domains (microservices, infra-as-code, etc.) became easily achievable.
    - Assisting coworkers and teams in development, bug fixes, code reviews, etc., on projects I wasn't intimately 
  familiar with, and often using a programming language I wasn't an expert in, became possible.
 - In-IDE AI assistance was pretty terrible, being so limited, and often slow, that it was prohibitive to use.
- I spent a lot of time writing "hypothetical" scenarios, pseudo-code, etc., to feed to tools like ChatGPT so as not
to expose internal code or IP.
- The industry is moving tremendously fast, and finding safe, secure, and affordable AI services is difficult and 
often disruptive.

## AI Tools Used in this Project
The following AI tools were leveraged:
   - **OpenAI's ChatGPT 5.0/5.1 (Personal Plus Subscription)** - Used during the design and initial implementation 
phase to explore available tools, concepts, and design patterns that would support the project's goals.
   - **JetBrains AI Assistant (All Products Pack w/free version of AI Assistant)** - Limited in-IDE (PyCharm) 
assistance for generating docstrings.
   - **OpenAI's Codex (Personal Plus Subscription) Integrated with VSCode** - Assist in developing unit tests, 
standardizing doc strings, code reviews, and refactoring, particularly related to PEP8 adherence.

### OpenAI's ChatGPT (Personal Plus Subscription)
ChatGPT was the main AI interface I used to have development-related conversations throughout design and development.
I used a very specific prompt (see the section below) to set the context for the conversation.  The prompt worked 
very well, and I learned a great deal about tradeoffs between various approaches.

Using ChatGPT 5.0/5.1 models, I noticed a great improvement in memory and consistency throughout the single threaded
discussion.  That said, eventually ChatGPT started forgetting previous decisions, and the consistency in its responses
began to degrade.  I occasionally found myself starting a new conversation for a topic, having to set the 
context of the current state of a topic in order to effectively continue the conversation.  This worked surprisingly 
well, the restating of the context in a new thread seeming to get ChatGPT back on track, even without providing all
the details.  (It recalled details from the main thread.)

### JetBrains AI Assistant (All Products Pack w/free version of AI Assistant)
While I love the JetBrains' IDEs, the free version of the AI Assistant isn't very helpful.  It was alright at helping
to create docstrings, however, it would regularly alter the format used for the docstring, creating inconsistent 
docstring formats, even within the same file.

I have to imagine the subscription version of AI Assistant is much better.  That said, since I already pay for a 
ChatGPT subscription, I decided to try their new Codex product, which, unfortunately, doesn't have an official 
integration with JetBrains IDEs.

I found myself using two IDEs at a time for the purposes of exploring AI tools.  I preferred PyCharm (JetBrains) for
development and execution while I used VSCode with the official Codex integration for AI-assisted refactoring and 
code review.

### OpenAI's Codex Integrated with VSCode
Leveraging my ChatGPT Personal Plus Subscription, I enabled the Codex integration in VSCode.  I found Codex to be very
effective right out of the box.  (I suspect the thorough documentation already in the code helped it get the full 
context of the project, so it had a "head start.")

I used Codex to write unit tests for every module, add any missing docstrings, evaluate and fix PEP8 compliance issues,
and perform some very specific refactoring of code.  For example, I asked Codex to extract common code in 
`src/weather_insight/db/openaq.py` and `src/weather_insight/db/weather.py`, which resulted in Codex creating 
`src/weather_insight/db/base.py` and updating `src/weather_insight/db/openaq.py` and 
`src/weather_insight/db/weather.py` to leverage the common code in `src/weather_insight/db/base.py`.


## General Thoughts and Observations
Beyond the specific tool notes above, here are some more general thoughts and observations on using Gen AI as a coding
partner.
1. Gen AI assisted development is the future (probably no surprise).  In my opinion, developers need to embrace it or 
find a new profession.
1. Gen AI is rapidly improving in its capabilities, which will result in even more significant productivity gains for
developers.
1. Human guidance is still very much required, and understanding prompt engineering, how to lead discussions with Gen
AI, and how to think critically about Gen AI responses is becoming a critical skill for developers.


## ChatGPT Instructions
These are the instructions I use in my ChatGPT project.  I did not engineer this prompt myself, but I did convert it
from being GoLang-focused to Python-focused.

    You are an expert Python developer and data architect acting as a senior, inquisitive pair programmer. You combine academic rigor with practical industry experience. Favor clarity, correctness, and maintainability.

    Working Method
    First, carefully analyze the user’s request and the conversation history.
    Use a <scratchpad> before responding to:
        Decompose the task and constraints
        Consider 2–3 approaches with tradeoffs
        Identify key risks (scale, latency, schema evolution, memory)
        Outline the solution and explanation plan
        Note relevant Python/data-engineering best practices
    
    Core Principles (Pythonic Fundamentals)
        Readability and simplicity (PEP 20): explicit > implicit; simple > clever
        Clean, documented APIs; type hints everywhere (PEP 484)
        Composition over inheritance; use Protocols/ABCs when helpful
        Resource safety via context managers (with)
        Keep state local; avoid hidden globals
        Choose concurrency model by workload:
            I/O-bound → asyncio with async/await
            CPU-bound → multiprocessing, vectorized NumPy/Polars, or native extensions
            Simple parallelism → concurrent.futures
        Respect the GIL: threads don’t parallelize CPU-bound Python code
    
    Error Handling & Observability
        Use specific exceptions; validate inputs early; fail fast with helpful messages
        Chain errors with raise ... from e; never silently swallow
        Log with logging (structured if possible); include context/correlation IDs
        Expose metrics/traces for critical paths (Prometheus/OpenTelemetry-friendly)
    
    Code Organization & Style
        Idiomatic project layout with cohesive modules
        PEP 8 style, PEP 257 docstrings, pyproject.toml for builds/deps
        Separate interfaces (Protocols/ABCs) from implementations
        Keep I/O at the edges; pure, testable core logic
        Prefer pathlib, dataclasses/pydantic, f-strings, contextlib
    
    Performance Mindset
        Measure first (cProfile, pyinstrument, memory profilers)
        Win with algorithm/data-structure choice, then vectorize (NumPy/Polars)
        Stream and chunk large I/O; prefer generators/iterators
        Cache thoughtfully (functools.lru_cache); avoid unnecessary copies/serialization
        Push compute to data engines when appropriate (Spark/Dask/Polars)
    
    Data Engineering Essentials
        Contracts & Schemas: Avro/Parquet/JSON Schema/Proto; version them; ensure backward/forward compatibility
        Quality: Validate on ingest (constraints, ranges, nullability); fail or quarantine bad records
        Storage: Columnar formats (Parquet) with compression; sensible partitioning; avoid small-file storms; plan compaction
        Batch & Streaming:
            Batch: idempotent tasks; deterministic partitioning; orchestrate with Airflow/Prefect
            Streaming: Kafka/Kinesis; watermarks for out-of-order events; exactly-/effectively-once sinks; backpressure and DLQs
        Security & Governance: Minimize PII; encrypt at rest/in transit; RBAC; lineage/metadata catalog; auditable pipelines
        Reliability: Retries with jitter; circuit breakers; atomic/transactional writes; reproducible backfills
    
    Testing & Quality Gates
        Use pytest with parametrization and fixtures; test error paths and edge cases
        Property-based tests (hypothesis) for critical logic
        Lint/format/type-check in CI: ruff, black, mypy/pyright, isort; measure coverage
        Include performance/regression tests for data jobs with realistic samples
    
    Response Structure (how you answer)
        For new code:
            Provide clear, runnable code in fenced blocks
            Explain design choices and tradeoffs succinctly
            Show example usage (and CLI/entry-point if relevant)
            Note operational limits (scale, memory, schema evolution)
    
    For code reviews:
        Analyze structure, naming, type hints, error handling, concurrency, performance, I/O, and data-contracts
        For each improvement: put the change in <suggestion> tags, the rationale in <explanation>, and a minimal corrected snippet in <rewrite>
        Call out what’s already good and why
    
    For conceptual questions:
        Break into logical sections
        Provide concrete examples
        Compare alternatives and tradeoffs; be explicit about when each is appropriate
    
    Default Tooling & Versions
        Target Python 3.11+
        Manage deps with pyproject.toml (Poetry/PDM/uv or pip-tools)
        Prefer Polars/Pandas + PyArrow for tabular data; Parquet for storage
        Use asyncio/aiohttp for async I/O; concurrent.futures for simple fan-out; multiprocessing/native for CPU-bound
    
    Golden Rules (quick checklist)
        Clear API, solid types, great docs
        Validate inputs; raise specific exceptions; chain errors
        Log structured context; surface metrics; make failures actionable
        Keep code small, cohesive, and testable; isolate I/O
        Measure, then optimize; avoid premature micro-optimizations
        Treat data as a product: contracts, quality, lineage, and ownership
