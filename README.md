# MLRQ

**MLRQ** (Machine Learning Redis Queue) is a minimalist task distribution and job processing library built on Redis. Unlike heavyweight solutions (e.g. Airflow, Celery, Ray), **MLRQ** focuses on simplicity and minimal overhead—perfect for quick ML experiments, model serving, or research setups.

**Key Idea:** Decorate a Python function so that calls to it enqueue tasks in Redis. A separate worker fetches and executes these tasks, storing their results back in Redis. This setup lets you offload ML computations to a dedicated machine or process—great if you have a loaded model on a GPU server while you enqueue tasks from a lightweight client machine.

## Installation

Prerequisites:  
- Python 3.10+
- Redis server running locally or accessible remotely

Install via pip (recommended if you have a `pyproject.toml`):
```bash
pip install mlrq
```

## Getting Started with two scripts:

```python
# worker.py

import time
from redis import Redis
from mlrq import Worker, distribute

@distribute()
def word_count_processor(text):
    """
    Counts the number of words in the given text.
    """
    time.sleep(1)
    return len(text.split())

def main():
    """
    Start the MLRQ worker to serve text processing jobs.
    """
    rclient = Redis(host="localhost", port=6379, db=0)

    worker = Worker(
        rclient,
        implements={word_count_processor: {}},  # No additional dependencies required
    )

    worker.run(stop_condition=lambda: False)  # Infinite serving loop

if __name__ == "__main__":
    main()
```

Once the worker machine (which has the model loaded) processes the job, results are stored in Redis. The client can retrieve results:

```python
# client.py

from redis import Redis
# from .worker import word_count_processor   # Only function signature is required, you can either import it or write it:
from mlrq import distribute

@distribute()
def word_count_processor(text):
    ...

# Connect to Redis
rclient = Redis(host="localhost", port=6379, db=0)

# Example input text
text = "This is a simple demo of the MLRQ system."

# Enqueue the job
job = word_count_processor(rclient, text=text)

# Check and fetch results
while not job.ready():
    time.sleep(0.1) # wait a bit...

result = job.get()
```

## Batching for ML Efficiency

Many ML models run faster in batches. You can tell MLRQ to batch multiple enqueued calls:

```python
# worker.py

import time
from redis import Redis
from mlrq import Worker, distribute


@distribute(max_batch_size=5, batch_on="numbers")
def batch_square_processor(numbers):
    """
    Processes a batch of numbers and returns their squares.
    """
    time.sleep(1)
    return [n ** 2 for n in numbers]

... # run worker/serve as before

```

```python
# client.py
from redis import Redis
from mlrq import distribute
# from .worker import batch_square_processor  # Only function signature is required, you can either import it or write it:

@distribute(max_batch_size=5, batch_on="numbers")
def batch_square_processor(numbers):
    ...

# Connect to Redis
rclient = Redis(host="localhost", port=6379, db=0)

# Example input: Single numbers (which will be batched on the server)
# Note: if the worker is too fast, not everything might batch. It will at _most_ batch up to max_batch_size
numbers_to_process = [2, 3, 5, 7, 11]

# Enqueue the jobs
jobs = []
for number in numbers_to_process:
    job = batch_square_processor(rclient, numbers=[number])  # Wrap single number as list
    jobs.append(job)

# Check and fetch results
for i, job in enumerate(jobs):
    while not job.ready():
        time.sleep(0.1) # wait a bit...
    # you don't have to do this serially, but it helps
    result = job.get()
    print(f"The square of {numbers_to_process[i]} is: {result}")
```

This can greatly reduce overhead and improve throughput, especially for deep learning models (batched inference or training).

## Why MLRQ for ML?

- **Simplicity:** Just add a decorator. No complex DAGs or configurations.
- **Easy Model Serving:** Keep a model in memory on a GPU server and push inference requests from anywhere.
- **Batching:** Improve performance by grouping requests into single model calls.
- **Minimal Dependencies:** Just Python and Redis.

## License

This project is licensed under the MIT License.
