from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import httpx
import asyncio
import uuid
from datetime import datetime

app = FastAPI(title="Task Automation API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── In-memory storage ────────────────────────────────────────────────────────
task_logs: Dict[str, Any] = {}


# ─── Models ───────────────────────────────────────────────────────────────────

class Task(BaseModel):
    task_type: str
    payload: Dict[str, Any]
    retry_count: int = 3


class WebhookTrigger(BaseModel):
    webhook_url: str
    task: Task


class ChainedTask(BaseModel):
    """Task chaining — output of one task becomes input of the next."""
    tasks: List[Task]


class BatchRequest(BaseModel):
    """Batch execution — run multiple tasks at once."""
    tasks: List[Task]


class ScheduledTask(BaseModel):
    """Scheduled task — run after a delay."""
    task: Task
    delay_seconds: int = 5


# ─── Task Executors ───────────────────────────────────────────────────────────

async def execute_task(task: Task, input_data: Dict[str, Any] = {}) -> Dict[str, Any]:
    """Execute a task based on its type. Accepts input_data for chaining."""
    task_type = task.task_type.lower()

    # Merge input_data into payload for chaining support
    payload = {**task.payload, **input_data}

    # EMAIL TASK
    if task_type == "email":
        to = payload.get("to", "user@example.com")
        subject = payload.get("subject", "No Subject")
        body = payload.get("body", payload.get("output", ""))
        return {
            "status": "sent",
            "to": to,
            "subject": subject,
            "body": body,
            "message": f"Email sent to {to} with subject '{subject}'"
        }

    # DATA TRANSFORM TASK
    elif task_type == "data_transform":
        data = payload.get("data", payload.get("output", ""))
        operation = payload.get("operation", "uppercase")
        if operation == "uppercase":
            result = str(data).upper()
        elif operation == "lowercase":
            result = str(data).lower()
        elif operation == "reverse":
            result = str(data)[::-1]
        else:
            result = str(data)
        return {"status": "transformed", "input": data, "output": result}

    # API CALL TASK
    elif task_type == "api_call":
        url = payload.get("url", "https://jsonplaceholder.typicode.com/posts/1")
        method = payload.get("method", "GET").upper()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                if method == "GET":
                    response = await client.get(url)
                else:
                    response = await client.post(url, json=payload)
            return {
                "status": "success",
                "status_code": response.status_code,
                "output": response.json()
            }
        except Exception as e:
            raise Exception(f"API call failed: {str(e)}")

    # MATH TASK
    elif task_type == "math":
        operation = payload.get("operation", "add")
        a = float(payload.get("a", payload.get("result", 0)))
        b = float(payload.get("b", 0))
        if operation == "add":
            result = a + b
        elif operation == "subtract":
            result = a - b
        elif operation == "multiply":
            result = a * b
        elif operation == "divide":
            if b == 0:
                raise Exception("Division by zero!")
            result = a / b
        else:
            result = a + b
        return {"status": "calculated", "operation": operation, "a": a, "b": b, "result": result, "output": result}

    else:
        raise Exception(f"Unknown task type: {task_type}")


# ─── Retry Logic ──────────────────────────────────────────────────────────────

async def execute_with_retry(task: Task, input_data: Dict[str, Any] = {}) -> Dict[str, Any]:
    """Execute task with retry logic on failure."""
    last_error = None

    for attempt in range(1, task.retry_count + 1):
        try:
            result = await execute_task(task, input_data)
            result["attempt"] = attempt
            return result
        except Exception as e:
            last_error = str(e)
            print(f"Attempt {attempt} failed: {last_error}")
            if attempt < task.retry_count:
                await asyncio.sleep(1)

    raise Exception(f"All {task.retry_count} attempts failed. Last error: {last_error}")


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/")
def read_root():
    return {"message": "Task Automation API is running!", "version": "2.0.0"}


@app.post("/tasks/execute")
async def execute_task_endpoint(task: Task):
    """Execute a single task immediately."""
    task_id = str(uuid.uuid4())
    started_at = datetime.now().isoformat()

    try:
        result = await execute_with_retry(task)
        log = {
            "task_id": task_id,
            "task_type": task.task_type,
            "status": "success",
            "started_at": started_at,
            "completed_at": datetime.now().isoformat(),
            "result": result
        }
        task_logs[task_id] = log
        return log

    except Exception as e:
        log = {
            "task_id": task_id,
            "task_type": task.task_type,
            "status": "failed",
            "started_at": started_at,
            "completed_at": datetime.now().isoformat(),
            "error": str(e)
        }
        task_logs[task_id] = log
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/tasks/chain")
async def chain_tasks(chained: ChainedTask):
    """
    Task Chaining — output of one task becomes input of the next.
    Similar to pipeline execution in AI Workflow Pipeline Builder.
    """
    chain_id = str(uuid.uuid4())
    started_at = datetime.now().isoformat()
    chain_log = []
    current_input = {}

    for i, task in enumerate(chained.tasks):
        task_id = str(uuid.uuid4())
        try:
            result = await execute_with_retry(task, current_input)
            chain_log.append({
                "step": i + 1,
                "task_id": task_id,
                "task_type": task.task_type,
                "status": "success",
                "output": result
            })
            # Pass output to next task as input
            current_input = result

        except Exception as e:
            chain_log.append({
                "step": i + 1,
                "task_id": task_id,
                "task_type": task.task_type,
                "status": "failed",
                "error": str(e)
            })
            # Stop chain on failure
            break

    log = {
        "chain_id": chain_id,
        "total_tasks": len(chained.tasks),
        "completed_tasks": len([t for t in chain_log if t["status"] == "success"]),
        "started_at": started_at,
        "completed_at": datetime.now().isoformat(),
        "chain_log": chain_log,
        "final_output": current_input
    }
    task_logs[chain_id] = log
    return log


@app.post("/tasks/batch")
async def batch_execute(batch: BatchRequest):
    """
    Batch Execution — run multiple tasks in parallel at once.
    """
    batch_id = str(uuid.uuid4())
    started_at = datetime.now().isoformat()

    # Run all tasks in parallel using asyncio.gather
    async def run_single(task: Task, index: int):
        task_id = str(uuid.uuid4())
        try:
            result = await execute_with_retry(task)
            return {
                "index": index,
                "task_id": task_id,
                "task_type": task.task_type,
                "status": "success",
                "result": result
            }
        except Exception as e:
            return {
                "index": index,
                "task_id": task_id,
                "task_type": task.task_type,
                "status": "failed",
                "error": str(e)
            }

    results = await asyncio.gather(*[run_single(task, i) for i, task in enumerate(batch.tasks)])

    log = {
        "batch_id": batch_id,
        "total_tasks": len(batch.tasks),
        "successful": len([r for r in results if r["status"] == "success"]),
        "failed": len([r for r in results if r["status"] == "failed"]),
        "started_at": started_at,
        "completed_at": datetime.now().isoformat(),
        "results": list(results)
    }
    task_logs[batch_id] = log
    return log


@app.post("/tasks/schedule")
async def schedule_task(scheduled: ScheduledTask, background_tasks: BackgroundTasks):
    """
    Scheduled Task — run a task after a delay (in seconds).
    """
    task_id = str(uuid.uuid4())

    async def delayed_run():
        await asyncio.sleep(scheduled.delay_seconds)
        try:
            result = await execute_with_retry(scheduled.task)
            task_logs[task_id] = {
                "task_id": task_id,
                "task_type": scheduled.task.task_type,
                "status": "success",
                "delay_seconds": scheduled.delay_seconds,
                "completed_at": datetime.now().isoformat(),
                "result": result
            }
        except Exception as e:
            task_logs[task_id] = {
                "task_id": task_id,
                "task_type": scheduled.task.task_type,
                "status": "failed",
                "delay_seconds": scheduled.delay_seconds,
                "completed_at": datetime.now().isoformat(),
                "error": str(e)
            }

    background_tasks.add_task(delayed_run)
    return {
        "task_id": task_id,
        "message": f"Task scheduled to run after {scheduled.delay_seconds} seconds!",
        "status": "scheduled"
    }


@app.post("/webhook/trigger")
async def webhook_trigger(trigger: WebhookTrigger, background_tasks: BackgroundTasks):
    """Trigger a task via webhook — executes in background."""
    task_id = str(uuid.uuid4())

    async def run_and_callback():
        try:
            result = await execute_with_retry(trigger.task)
            status = "success"
            error = None
        except Exception as e:
            result = None
            status = "failed"
            error = str(e)

        task_logs[task_id] = {
            "task_id": task_id,
            "task_type": trigger.task.task_type,
            "status": status,
            "result": result,
            "error": error
        }

    background_tasks.add_task(run_and_callback)
    return {
        "task_id": task_id,
        "message": "Task triggered! Running in background.",
        "status": "queued"
    }


@app.get("/tasks/logs")
def get_all_logs():
    """Get execution logs for all tasks."""
    return {
        "total_tasks": len(task_logs),
        "logs": list(task_logs.values())
    }


@app.get("/tasks/logs/{task_id}")
def get_task_log(task_id: str):
    """Get execution log for a specific task."""
    if task_id not in task_logs:
        raise HTTPException(status_code=404, detail="Task not found")
    return task_logs[task_id]


@app.get("/tasks/stats")
def get_stats():
    """
    Monitor automation pipeline health.
    Shows success/failure counts and success rate.
    Useful for QA and reliability monitoring.
    """
    logs = list(task_logs.values())
    total = len(logs)

    if total == 0:
        return {
            "total_tasks": 0,
            "successful": 0,
            "failed": 0,
            "success_rate": "0%",
            "task_type_breakdown": {}
        }

    successful = len([l for l in logs if l.get("status") == "success"])
    failed = len([l for l in logs if l.get("status") == "failed"])
    success_rate = round((successful / total) * 100, 2)

    # Break down by task type
    breakdown = {}
    for log in logs:
        task_type = log.get("task_type", log.get("chain_id") and "chain" or "batch")
        if task_type not in breakdown:
            breakdown[task_type] = {"total": 0, "successful": 0, "failed": 0}
        breakdown[task_type]["total"] += 1
        if log.get("status") == "success":
            breakdown[task_type]["successful"] += 1
        else:
            breakdown[task_type]["failed"] += 1

    return {
        "total_tasks": total,
        "successful": successful,
        "failed": failed,
        "success_rate": f"{success_rate}%",
        "task_type_breakdown": breakdown
    }


@app.delete("/tasks/logs")
def clear_logs():
    """Clear all execution logs — useful for testing and QA."""
    task_logs.clear()
    return {"message": "All logs cleared successfully!"}