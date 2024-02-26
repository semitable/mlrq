import functools
import json
import uuid
from datetime import datetime
from typing import Callable, Literal

from redis import Redis

_job_prefix = "mlrq:job"
_job_q_prefix = "mlrq:job_queue"
_expiration_time = 3600


def distribute(
    _func: Callable | None = None,
    *,
    max_batch_size: int = 1,
    batch_on: Literal["__all__"] | list[str] = "__all__",
    priority: Literal["low", "normal", "high"] = "normal",
) -> Callable:
    """
    Decorator to distribute function calls as jobs through Redis.

    :param _func: The function to be distributed. Default is None.
    :param max_batch_size: Maximum number of jobs to batch together. Default is 1.
    :param batch_on: Criteria to batch jobs on. Default is None.
    :param priority: Job priority, can be 'high', 'normal', or 'low'. Default is 'normal'.
    :return: Decorated function.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapped(rclient, **kwargs):
            # Create a job instance with the provided arguments
            job = Job(
                func,
                rclient,
                priority=priority,
                max_batch_size=max_batch_size,
                batch_on=batch_on,
                args=kwargs,
            )
            # Enqueue the job for processing
            job.enqueue()
            return job

        return wrapped

    if _func is None:
        return decorator
    else:
        return decorator(_func)


class Job:
    """A class representing a distributed job."""

    def __init__(
        self,
        func: Callable,
        rclient: Redis,
        priority: Literal["low", "normal", "high"],
        max_batch_size: int,
        batch_on: Literal["__all__"] | list[str],
        args,
    ) -> None:
        self.job_id = str(uuid.uuid4())  # Generate a unique job ID
        self.func_name = func.__name__
        self.args = args
        self.rclient = rclient

        self.job_q = f"{_job_q_prefix}:{priority}:{self.func_name}"
        self.job_key = f"{_job_prefix}:{self.job_id}"

        self.max_batch_size = max_batch_size
        self.batch_on = batch_on

    def enqueue(self) -> bool:
        """Enqueues the job in Redis."""
        func_args_description = ", ".join(f"{k}={v}" for k, v in self.args.items())

        data = {
            "enqueued_at": str(datetime.now()),
            "function": self.func_name,
            "description": f"{self.func_name}({func_args_description})",
            "batch": json.dumps(
                {"max_batch_size": self.max_batch_size, "batch_on": self.batch_on}
            ),
            "args": json.dumps(self.args),
        }

        self.rclient.hset(self.job_key, mapping=data)
        self.rclient.rpush(self.job_q, self.job_id)
        print(f"Job {self.job_key} pushed in queue {self.job_q}")

        self.rclient.expire(self.job_key, _expiration_time)
        self.rclient.expire(self.job_q, _expiration_time)

        return True

    def ready(self):
        """Checks if the job is ready (completed)."""
        return self.rclient.hexists(self.job_key, "ended_at")

    def get(self):
        """Retrieves the result of the job."""
        return json.loads(self.rclient.hgetall(self.job_key)["result"])


class Worker:
    def __init__(
        self,
        rclient: Redis,
        implements: list[Callable],
        priorities: list[str] = ["high", "normal", "low"],
    ) -> None:
        self.rclient = rclient
        self.functions = implements
        self.queues = []
        for priority in priorities:
            for func in implements:
                self.queues.append(f"{_job_q_prefix}:{priority}:{func.__name__}")

    def process_batch(self, job, job_key, queue):
        jobs = [job]
        job_keys = [job_key]

        batch = json.loads(job["batch"])

        max_batch_size = batch["max_batch_size"]
        batch_on = batch["batch_on"]

        # start picking up tasks on the same queue
        while len(jobs) < max_batch_size:
            new_job_key = self.rclient.lpop(queue)
            if new_job_key is None:
                break
            new_job_key = f"{_job_prefix}:{new_job_key}"
            new_job = self.rclient.hgetall(new_job_key)
            jobs.append(new_job)
            job_keys.append(new_job_key)

        args_list = [json.loads(j["args"]) for j in jobs]

        args_list = [
            {
                key: arg[key]
                for key in arg.keys()
                if batch_on == "__all__" or key in batch_on
            }
            for arg in args_list
        ]

        kwargs = {
            key: [d[key] for d in args_list if key in d]
            for key in set(k for d in args_list for k in d)
        }

        kwargs = {**json.loads(job["args"]), **kwargs}

        for func in self.functions:
            if func.__name__ == job["function"]:
                results = func.__wrapped__(**kwargs)
                break
        else:
            print("Function not implemented. Bug?")
            raise ValueError

        for job, job_key, result in zip(jobs, job_keys, results):
            self.rclient.hset(
                job_key,
                mapping={"result": json.dumps(result), "ended_at": str(datetime.now())},
            )
            self.rclient.expire(job_key, _expiration_time)

    def process_single_job(self, job, job_key):
        kwargs = json.loads(job["args"])

        for func in self.functions:
            if func.__name__ == job["function"]:
                result = func.__wrapped__(**kwargs)
                break
        else:
            print("Function not implemented. Bug?")
            raise ValueError

        self.rclient.hset(
            job_key,
            mapping={"result": json.dumps(result), "ended_at": str(datetime.now())},
        )
        self.rclient.expire(job_key, _expiration_time)

    def run(self):
        print(f"Listening on {self.queues}")

        while True:
            queue, job_id = self.rclient.blpop(self.queues)
            job_key = f"{_job_prefix}:{job_id}"
            job = self.rclient.hgetall(job_key)

            batch = json.loads(job["batch"])
            if batch["max_batch_size"] > 1:
                self.process_batch(job, job_key, queue)
            else:
                self.process_single_job(job, job_key)
