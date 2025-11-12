# QueueCTL
### CLI-based Background Job Queue System  

**Author:** Abhyudaya Sharma (PES2UG22CS022)  
**Tech Stack:** Python, SQLite, Multiprocessing  

---

## 1️⃣ Setup Instructions — How to run locally

### Prerequisites
- Python 3.10 or later  
- Windows PowerShell or CMD  

### Steps
bash
# Clone repository
git clone https://github.com/<your-username>/queuectl.git
cd queuectl

# (Optional) Create virtual environment
python -m venv venv
venv\Scripts\activate

# Verify installation
python queuectl.py --help


Usage Examples — CLI commands with example outputs
Successful Job
python queuectl.py enqueue "echo hello world"
python queuectl.py worker start --count 1
python queuectl.py status
python queuectl.py list --state completed

 Failed Job → Retries → DLQ
python queuectl.py enqueue "no_such_command_foo" --max-retries 3
python queuectl.py worker start --count 1
python queuectl.py dlq list


Example output:

7ff6f8f4-ba02-44ff-8730-c723168afafe dead after 3 attempts | last_error=Exit code 1

 Retry a DLQ Job
python queuectl.py dlq retry 7ff6f8f4-ba02-44ff-8730-c723168afafe
python queuectl.py worker start --count 1

 Persistence Test
for ($i = 1; $i -le 5; $i++) {
    python queuectl.py enqueue "python -c 'import time; time.sleep(3)'"
}
python queuectl.py worker start --count 1
# Stop midway (Ctrl + C)
python queuectl.py worker start --count 1  # Resumes pending jobs

 Scheduled Job
python queuectl.py enqueue "echo future" --run-at 2025-12-31T23:59:00Z
python queuectl.py list --state pending

 Architecture Overview — Job lifecycle, data persistence, worker logic

Components

1) Job Store: SQLite database (queue.db) holds job metadata and state.

2) Worker: Independent processes that poll the DB for pending jobs.

3) Retry Logic: Failed jobs retry automatically with exponential backoff delay = base ^ attempts.

4) Dead Letter Queue (DLQ): Jobs exceeding retry limit move to DLQ for manual retry.

5) CLI: Provides commands to enqueue, list, retry, and configure the system.

Lifecycle

1) enqueue → job created as pending

2) worker picks job, marks it processing

3) Executes command

4) If success → completed; if failure → retry after backoff

5) After max retries → move to dead

6) User can dlq retry → job returns to pending

Persistence

1) All state is stored in queue.db, so data survives restarts.

2) Each worker uses a new SQLite connection (Windows-safe).

3) Graceful shutdown ensures jobs finish before process exit.

Assumptions & Trade-offs

| Aspect          | Decision          | Reason                                       |
| --------------- | ----------------- | -------------------------------------------- |
| **Storage**     | SQLite            | Simple and persistent across restarts        |
| **Concurrency** | Multiprocessing   | True parallel job execution on Windows       |
| **Config**      | Stored in DB      | Easier persistence vs. environment variables |
| **Backoff**     | `base ^ attempts` | Simple exponential delay                     |
| **No Web UI**   | CLI-only          | Focus on backend logic                       |

Testing Instructions — How to verify functionality
| Test               | Command                                            | Expected Outcome          |
| ------------------ | -------------------------------------------------- | ------------------------- |
| **Successful job** | `python queuectl.py enqueue "echo hi"`             | Moves to `completed`      |
| **Failing job**    | `python queuectl.py enqueue "no_such_command_foo"` | Retries & DLQ             |
| **DLQ retry**      | `python queuectl.py dlq retry <id>`                | Moves back to `pending`   |
| **Persistence**    | Stop & restart worker                              | Jobs resume automatically |
| **Scheduling**     | `--run-at` flag                                    | Executes at future time   |


Demo Video

Watch the working CLI demo here
https://drive.google.com/file/d/1rWIEnUcl3-tCoj42f6iihlMQm9ycV02X/view?usp=sharing


