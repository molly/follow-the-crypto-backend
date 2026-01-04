# Individual Management Commands

This directory contains command-line utilities to manage individuals in the Follow the Crypto dataset without needing to manually edit Firestore documents or run the entire pipeline.

## ⚠️ Important: Company Dependencies

**When adding individuals, the system automatically handles company data dependencies:**

- If an individual lists companies in their `company` field, those companies' data will be updated
- Company `relatedIndividuals` lists are refreshed
- Company contribution totals are recalculated to include the new individual's contributions
- Company party summaries are updated

This means adding one individual may trigger updates to multiple company records. The commands handle this automatically but it's important to understand the scope of changes.

## Available Commands

### 1. Add Individual (`add_individual.py`)

Add a new individual to track with optional immediate data fetching.

```bash
# Basic usage - adds and fetches data immediately
python -m commands.add_individual --id "john-doe" --name "John Doe" --zip "12345"

# With company association
python -m commands.add_individual --id "jane-smith" --name "Jane Smith" --company "Coinbase" --title "CEO"

# With employer search terms
python -m commands.add_individual --id "bob-jones" --employer-search "crypto,blockchain,bitcoin" --city "San Francisco"

# Add without immediate data fetching
python -m commands.add_individual --id "alice-wilson" --name "Alice Wilson" --zip "94105" --no-fetch
```

**Options:**

- `--id` (required): Unique identifier (e.g., 'john-doe')
- `--name`: Display name (defaults to formatted ID)
- `--name-search`: Alternative name for searching contributions
- `--zip`: ZIP code for contribution search
- `--city`: City for efiled contribution search
- `--employer-search`: Comma-separated employer search terms
- `--company`: Comma-separated associated companies
- `--title`: Job title
- `--photo-credit`: Photo credit URL
- `--no-fetch`: Don't fetch contribution data immediately

### 2. Fetch Individual Data (`fetch_individual.py`)

Fetch contribution data for an existing individual.

```bash
# Fetch data for specific individual
python -m commands.fetch_individual --id "john-doe"

# Force refetch even if data exists
python -m commands.fetch_individual --id "jane-smith" --force
```

**Options:**

- `--id` (required): Individual ID to fetch data for
- `--force`: Refetch even if data already exists

### 3. List Individuals (`list_individuals.py`)

Display all tracked individuals and their data status.

```bash
# Basic list
python -m commands.list_individuals

# Detailed view
python -m commands.list_individuals --verbose
```

**Options:**

- `--verbose, -v`: Show detailed information including all fields

## Workflow Examples

### Adding a New Person (Traditional vs New Way)

**Old Way:**

1. Open Firestore console
2. Navigate to `constants/individuals`
3. Manually add JSON structure
4. Run entire pipeline: `python pipeline.py`
5. Wait for all individuals to be processed

**New Way:**

```bash
# One command adds and fetches data + updates company dependencies
python -m commands.add_individual --id "new-person" --name "New Person" --company "Crypto Corp" --zip "12345"
```

### Company Impact Example

```bash
# Adding an employee automatically updates the company page
python -m commands.add_individual --id "jane-crypto-ceo" --name "Jane Doe" --company "Crypto Corp" --title "CEO"
# ✅ Individual added
# 📊 Fetched 25 contributions
# 🔄 Processed contributions, found 3 new recipients
# 🏢 Updated company data, found 1 new company recipients
# ℹ️  Companies associated with jane-crypto-ceo: Crypto Corp
```

### Batch Operations

```bash
# Add multiple people without immediate fetching
python -m commands.add_individual --id "person-1" --name "Person One" --zip "12345" --no-fetch
python -m commands.add_individual --id "person-2" --name "Person Two" --zip "67890" --no-fetch

# Then fetch all at once using pipeline (also updates companies)
python pipeline.py --tasks complete_individual_workflow --individual-ids "person-1,person-2"
```

### Checking Status

```bash
# See who's tracked and their data status
python -m commands.list_individuals

# Get detailed view
python -m commands.list_individuals --verbose
```

## Field Reference

The commands create individual records with the same structure as existing data:

- `id`: Unique identifier (always matches the document key)
- `name`: Display name for the individual
- `nameSearch`: Alternative name for FEC searches (optional)
- `zip`: ZIP code for geographic contribution filtering
- `city`: City name for efiled contribution searches
- `company`: Array of associated company names
- `employerSearch`: Array of employer search terms
- `title`: Job title
- `photoCredit`: URL for photo attribution

## Dependencies

- Core: Uses existing Database and pipeline infrastructure
- Optional: `tabulate` for better table formatting (falls back gracefully)

## Dependency Cascade Handling

The commands automatically handle these data dependencies:

### Individual → Company Relationships

1. **relatedIndividuals lists**: When you add someone with a `company` field, that company's `relatedIndividuals` array is updated
2. **Company contribution totals**: The company's contribution summary includes the new individual's contributions
3. **Company party summaries**: Political party breakdowns are recalculated with the new data

### What Gets Updated

- ✅ `rawIndividualContributions` collection (individual's contribution data)
- ✅ `individuals` collection (processed individual data)
- ✅ `companies` collection (affected companies get updated `relatedIndividuals` and `contributions`)
- ✅ `allRecipients` collection (any new recipient committees discovered)

### Performance Impact

- Adding individual without company association: ~30 seconds
- Adding individual with company association: ~60-90 seconds (includes company reprocessing)
- Multiple individuals: Use `--no-fetch` then batch process with pipeline

## Error Handling

- Validates individual doesn't already exist before adding
- Checks for required fields
- Provides clear error messages
- Graceful fallbacks for missing dependencies
