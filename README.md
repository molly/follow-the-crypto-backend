# Follow the Crypto - Backend

Follow the cryptocurrency industry's influence on elections in the United States.

This is the Python backend that processes FEC data for [FollowTheCrypto.org](https://www.followthecrypto.org/). For the Next.js application that powers the website, see [follow-the-crypto](https://github.com/molly/follow-the-crypto).

## Quick Start

### List All Available Tasks
```bash
python pipeline.py --list-tasks
```

### Run the Entire Pipeline
```bash
python pipeline.py
```

### Run a Specific Task (and its dependencies)
```bash
python pipeline.py --tasks summarize_races
```

### Force Re-run Everything
```bash
python pipeline.py --force
```

### Dry Run (see what would execute)
```bash
python pipeline.py --dry-run
```

### Clear Cache and Re-run
```bash
python pipeline.py --clear-cache --force
```

## CLI Options

| Option | Description |
|--------|-------------|
| `--tasks TASK1,TASK2` | Run specific tasks and their dependencies |
| `--force` | Force re-run all tasks, ignoring completion state |
| `--dry-run` | Show execution plan without running tasks |
| `--clear-cache` | Clear HTTP request cache before running |
| `--verbose, -v` | Enable verbose logging |
| `--list-tasks` | List all available tasks and exit |
| `--continue-on-failure` | Continue execution even if a task fails |

## Architecture

### Directory Structure

```
follow-the-crypto-backend/
├── pipeline.py                 # CLI entry point
├── pipeline_core/              # Core pipeline infrastructure
│   ├── task.py                 # Task class and @task decorator
│   ├── registry.py             # Task registration system
│   ├── orchestrator.py         # Dependency resolution & execution
│   ├── state.py                # Firestore-based state tracking
│   └── context.py              # TaskContext for shared resources
├── tasks/                      # All pipeline tasks
│   ├── committees.py           # Committee-related tasks
│   ├── contributions.py        # Contribution tasks
│   ├── expenditures.py         # Expenditure tasks
│   ├── disbursements.py        # Disbursement tasks
│   ├── individuals.py          # Individual spending tasks
│   ├── companies.py            # Company spending tasks
│   ├── races.py                # Race tasks
│   ├── candidates.py           # Candidate tasks
│   ├── pacs.py                 # PAC tasks
│   ├── ads_task.py             # Ads tasks
│   └── recipients.py           # Recipient tasks
├── commands/                   # Standalone CLI utilities
│   ├── add_individual.py       # Add a new individual to track
│   ├── add_company.py          # Add a new company to track
│   ├── add_committee.py        # Add a new committee to track
│   └── ...                     # Other utility commands
├── company_utils.py            # Shared company processing utilities
├── get_missing_recipients.py   # Recipient enrichment logic
├── Database.py                 # Firestore client wrapper
└── [processing scripts]        # Core data processing logic
```

### Task Dependency Graph

```
hydrate_committees (root)
├── fetch_committee_contributions
│   └── process_committee_contributions
│       ├── get_top_pacs
│       └── summarize_races
├── fetch_committee_expenditures
│   └── process_expenditures
│       ├── update_candidate_expenditures
│       ├── update_outside_spending
│       └── summarize_races
├── fetch_committee_disbursements
├── fetch_individual_spending
│   └── process_individual_contributions
│       ├── summarize_recipients
│       └── summarize_races
├── fetch_company_spending
│   └── process_company_contributions
│       ├── summarize_recipients
│       └── summarize_races
└── fetch_ads

update_race_details (separate root)
└── summarize_races
    └── trim_candidates
        └── fetch_candidate_images
```

## Available Tasks

### Data Fetching Tasks

| Task | Description | Dependencies |
|------|-------------|--------------|
| `hydrate_committees` | Fetch committee details from FEC | None |
| `fetch_committee_contributions` | Fetch raw contributions | hydrate_committees |
| `fetch_committee_expenditures` | Fetch raw expenditures | hydrate_committees |
| `fetch_committee_disbursements` | Fetch disbursements | hydrate_committees |
| `fetch_individual_spending` | Fetch individual spending | hydrate_committees |
| `fetch_company_spending` | Fetch company spending | hydrate_committees |
| `update_race_details` | Fetch race details | None |
| `fetch_ads` | Fetch advertising data | hydrate_committees |
| `update_outside_spending` | Fetch outside spending | process_expenditures, update_race_details |

### Data Processing Tasks

| Task | Description | Dependencies |
|------|-------------|--------------|
| `process_committee_contributions` | Process contributions | fetch_committee_contributions |
| `process_expenditures` | Process expenditures | fetch_committee_expenditures |
| `process_individual_contributions` | Process individual data | fetch_individual_spending |
| `process_company_contributions` | Process company data | fetch_company_spending |
| `summarize_recipients` | Aggregate recipients | process_individual/company |
| `summarize_races` | Aggregate race data | process_expenditures, process_individual/company, update_race_details |
| `trim_candidates` | Remove low-spending candidates | summarize_races |

### Summary Tasks

| Task | Description | Dependencies |
|------|-------------|--------------|
| `get_top_pacs` | Get top fundraising PACs | process_committee_contributions |
| `update_candidate_expenditures` | Group expenditures by candidate | process_expenditures |
| `fetch_candidate_images` | Get candidates without images | trim_candidates |

## State Tracking

The pipeline automatically tracks task execution state in Firestore (collection: `_pipeline_state`). Each task stores:

- `status`: "running", "completed", or "failed"
- `started_at`: When the task started
- `completed_at`: When the task completed
- `result`: Any return value from the task
- `error`: Error message if failed

### Smart Skipping

Tasks are automatically skipped if:
1. They completed successfully in a previous run
2. Their input Firestore collections haven't changed since completion

Use `--force` to override this behavior.

## Command-Line Utilities

In addition to the pipeline, several standalone commands are available for managing tracked entities:

### Add Individual
```bash
python -m commands.add_individual \
  --id "john-doe" \
  --name "John Doe" \
  --zip "12345" \
  --employer-search "Coinbase,Crypto Corp"
```

### Add Company
```bash
python -m commands.add_company \
  --id "example-corp" \
  --name "Example Corp" \
  --category "exchange" \
  --country "USA"
```

### Add Committee
```bash
python -m commands.add_committee \
  --id "C00123456" \
  --description "Example PAC"
```

### List Entities
```bash
python -m commands.list_individuals
python -m commands.list_companies
python -m commands.list_committees
```

## Examples

### Run Just the Contribution Pipeline
```bash
python pipeline.py --tasks process_committee_contributions
```
This will run:
1. `hydrate_committees`
2. `fetch_committee_contributions`
3. `process_committee_contributions`

### Debug a Specific Task
```bash
python pipeline.py --tasks update_outside_spending --verbose --force
```

### Resume After Failure
If the pipeline fails partway through, just run it again:
```bash
python pipeline.py
```
It will automatically skip completed tasks and resume from where it failed.

### Run Multiple Specific Tasks
```bash
python pipeline.py --tasks "fetch_ads,get_top_pacs"
```

## Adding New Tasks

To add a new task to the pipeline:

1. Create a new file in `tasks/` (or add to an existing one):

```python
from pipeline_core.task import task

@task(
    name="my_new_task",
    depends_on=["some_other_task"],
    inputs=["inputCollection"],
    outputs=["outputCollection"],
)
def my_new_task(context):
    """Description of what this task does."""
    db = context.db
    session = context.session

    # Your logic here

    return {"some_stat": 123}
```

2. Import it in `tasks/__init__.py`:

```python
from . import my_new_module
```

3. The task will automatically be registered and available.

## Performance Optimizations

The codebase includes several optimizations for efficient data processing:

### 1. Constants Caching
Constants (committees, companies, individuals, etc.) are loaded once at pipeline startup and shared across all tasks via the `TaskContext`. This reduces Firestore reads by ~90%.

### 2. HTTP Response Caching
All FEC API requests are cached using `requests-cache` with filesystem backend. Repeated requests return instantly from cache, avoiding redundant API calls.

### 3. Incremental Recipient Enrichment
Recipient committee data is enriched incrementally:
- Only new recipients are fetched from the FEC API
- Previously enriched recipients are skipped
- Multiple tasks calling `get_missing_recipient_data()` benefit from shared state
- HTTP cache prevents duplicate API calls even if logic requests the same data

### 4. Error Handling
All Firestore document reads include proper error handling to gracefully handle missing documents:
```python
doc = db.client.collection("collection").document("doc_id").get()
data = doc.to_dict() if doc.exists else {}
```

## Troubleshooting

### Task Stuck in "running" State
If a task is marked as "running" but isn't actually running (e.g., due to a crash), clear its state:
```python
from Database import Database
from pipeline_core import StateTracker

db = Database()
tracker = StateTracker(db)
tracker.clear_state("task_name")
```

### Circular Dependency Error
The registry will detect circular dependencies at startup. Check the error message for which tasks are involved and fix the dependency declarations.

### Task Always Runs (Never Skips)
The smart skipping requires Firestore collections to have `updated_at` timestamps. If a collection doesn't have this field, tasks reading from it will always run.

### Import Errors
Ensure all dependencies are installed:
```bash
pip install -r requirements.txt
```

## Performance Notes

- Tasks run sequentially (not parallel) for simplicity and easier debugging
- HTTP responses are cached by `requests-cache` to avoid redundant API calls
- State tracking adds minimal overhead (one Firestore write per task)
- Constants are loaded once per pipeline run and shared across tasks
- Recipient enrichment is incremental and benefits from HTTP caching

## Development

### Project Structure
The codebase is organized into layers:
- **Pipeline Layer** (`pipeline_core/`, `tasks/`): Task orchestration and dependency management
- **Processing Layer** (root directory scripts): Core data processing logic
- **Utilities Layer** (`company_utils.py`, `get_missing_recipients.py`): Shared helper functions
- **Commands Layer** (`commands/`): Standalone CLI utilities
- **Data Layer** (`Database.py`): Firestore client and constants management

### Code Organization Principles
- Processing scripts should be pure functions that accept `db` and `session` parameters
- Tasks are thin wrappers that call processing scripts
- Shared logic should be extracted to utility modules
- Commands can load their own constants since they run standalone

## Future Enhancements

Potential improvements:
- Parallel execution for independent tasks
- Task result caching
- Web UI for monitoring pipeline execution
- Scheduled execution with Cloud Scheduler
- Notification on failures
- More granular state tracking (per-collection modification times)
- Batch Firestore operations for reading multiple documents
- Data models using Pydantic for type safety
