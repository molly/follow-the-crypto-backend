# Data Management Commands

This directory contains command-line utilities to manage individuals, companies, and committees in the Follow the Crypto dataset without needing to manually edit Firestore documents or run the entire pipeline.

## ⚠️ Important: Dependency Tracking

**The system automatically handles data dependencies across entities:**

### Individual → Company Dependencies
- If an individual lists companies in their `company` field, those companies' data will be updated
- Company `relatedIndividuals` lists are refreshed
- Company contribution totals are recalculated to include the new individual's contributions
- Company party summaries are updated

### Company → Individual Dependencies
- When adding a company, the system checks for existing individuals linked to it
- Linked individuals' contributions are automatically aggregated for the company
- Company data includes all related individuals and their contribution totals

### Committee Dependencies
- Committees track contributions from both individuals and companies
- Adding committees updates the `allRecipients` collection with FEC data
- Committee totals are fetched from FEC API when hydrated

This means adding one entity may trigger updates to multiple related records. The commands handle this automatically but it's important to understand the scope of changes.

## Available Commands

## Individual Management

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

### 4. Batch Daily Operations (`batch_daily.py`)

Optimized for daily operations - efficiently handles multiple individuals.

```bash
# Add multiple people for later processing
python -m commands.batch_daily --add "person1:Company A" --add "person2:Company B"

# Process all pending individuals
python -m commands.batch_daily --process-pending

# Add and process immediately
python -m commands.batch_daily --add "person1:Company A" --process-immediately
```

**Benefits for daily operations:**
- Selective company updates - only affected companies are reprocessed
- Batch processing - processes multiple individuals together
- Incremental updates - only processes individuals without data
- Time limits - respects daily operation time constraints

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
# Individual added
# Fetched 25 contributions
# Processed contributions, found 3 new recipients
# Updated company data, found 1 new company recipients
# Companies associated with jane-crypto-ceo: Crypto Corp
```

### Batch Operations

```bash
# Add multiple people without immediate fetching
python -m commands.add_individual --id "person-1" --name "Person One" --zip "12345" --no-fetch
python -m commands.add_individual --id "person-2" --name "Person Two" --zip "67890" --no-fetch

# Then fetch all at once using pipeline (also updates companies)
python pipeline.py --tasks complete_individual_workflow --individual-ids "person-1,person-2"
```

### Daily Operations Workflow

For frequent/daily additions, use the optimized batch commands:

```bash
# Add multiple people quickly (no immediate processing)
python -m commands.batch_daily --add "alice-ceo:Crypto Corp" --add "bob-cto:Crypto Corp"

# Process all pending in one efficient batch
python -m commands.batch_daily --process-pending

# Add and process immediately for urgent cases
python -m commands.batch_daily --add "urgent-person:Important Co" --process-immediately
```

### Checking Status

```bash
# See who's tracked and their data status
python -m commands.list_individuals

# Get detailed view
python -m commands.list_individuals --verbose
```

## Company Management

### 5. Add Company (`add_company.py`)

Add a new company to track with optional immediate data fetching.

```bash
# Basic usage - adds and fetches data immediately
python -m commands.add_company --id "example-corp" --name "Example Corp" --category "exchange"

# With multiple categories
python -m commands.add_company --id "crypto-ventures" --name "Crypto Ventures" --category "capital" --category "exchange"

# With full details
python -m commands.add_company --id "blockchain-inc" --name "Blockchain Inc" --category "other" --description "A blockchain company" --country "USA" --search-id "blockchain"

# Add without immediate data fetching
python -m commands.add_company --id "defi-corp" --name "DeFi Corp" --category "exchange" --no-fetch
```

**Options:**

- `--id` (required): Unique identifier (e.g., 'example-corp')
- `--name` (required): Company name
- `--category` (required, repeatable): Company category (exchange, capital, other, etc.)
- `--search-id`: Search identifier for FEC queries (defaults to lowercase name)
- `--description`: Company description
- `--country`: Country (defaults to USA)
- `--no-fetch`: Don't fetch contribution data immediately

**Automatic Dependency Handling:**

When adding a company with `--fetch`, the system will:
- Find all individuals linked to this company (via their `company` field)
- Aggregate their contributions to the company
- Update the company's `relatedIndividuals` list
- Process company contributions and discover new recipients

### 6. List Companies (`list_companies.py`)

Display all tracked companies and their data status.

```bash
# Basic list
python -m commands.list_companies

# Detailed view
python -m commands.list_companies --verbose
```

**Options:**

- `--verbose, -v`: Show detailed information including search IDs and descriptions

## Committee Management

### 7. Add Committee (`add_committee.py`)

Add a new committee to track with optional immediate FEC data fetching.

```bash
# Basic usage - adds and fetches FEC data immediately
python -m commands.add_committee --id "C00123456" --name "Example PAC" --description "A PAC focused on crypto policy"

# Minimal usage
python -m commands.add_committee --id "C00654321" --name "Tech Freedom Fund"

# Add without immediate data fetching
python -m commands.add_committee --id "C00999888" --name "Future PAC" --no-fetch
```

**Options:**

- `--id` (required): FEC Committee ID (format: C followed by 8 digits, e.g., 'C00123456')
- `--name` (required): Committee name
- `--description`: Committee description (HTML allowed)
- `--no-fetch`: Don't fetch FEC data immediately

**Automatic Data Fetching:**

When adding a committee with immediate fetch (default), the system will:
- Fetch committee details from FEC API (type, party, organization info)
- Fetch financial totals for the 2026 cycle (receipts, disbursements, etc.)
- Store complete committee data in the `committees` collection
- Update the committee's metadata in constants

### 8. List Committees (`list_committees.py`)

Display all tracked committees and their data status.

```bash
# Basic list
python -m commands.list_committees

# Detailed view with descriptions
python -m commands.list_committees --verbose
```

**Options:**

- `--verbose, -v`: Show detailed information including descriptions and FEC names

## Field Reference

### Individual Fields

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

### Company Fields

The commands create company records with the same structure as existing data:

- `id`: Unique identifier (always matches the document key)
- `name`: Display name for the company
- `search_id`: Search identifier used in FEC queries (defaults to lowercase name)
- `category`: Array of categories (e.g., 'exchange', 'capital', 'other')
- `description`: Text description of the company
- `country`: Country of operation (defaults to 'USA')

### Committee Fields

The commands create committee records with the same structure as existing data:

- `id`: FEC Committee ID (format: C########)
- `name`: Display name for the committee
- `description`: HTML description of the committee's purpose and focus

## Dependencies

- Core: Uses existing Database and pipeline infrastructure
- Optional: `tabulate` for better table formatting (falls back gracefully)

## Dependency Cascade Handling

The commands automatically handle these data dependencies:

### Individual → Company Relationships

1. **relatedIndividuals lists**: When you add someone with a `company` field, that company's `relatedIndividuals` array is updated
2. **Company contribution totals**: The company's contribution summary includes the new individual's contributions
3. **Company party summaries**: Political party breakdowns are recalculated with the new data

### Company → Individual Relationships

1. **Linked individuals**: When you add a company, the system finds all individuals with that company in their `company` field
2. **Contribution aggregation**: Individual contributions are aggregated to calculate company totals
3. **Related individuals list**: The company's `relatedIndividuals` array is populated with linked individuals

### Committee Tracking

1. **FEC data hydration**: Committees fetch detailed information from the FEC API including type, party, and financial data
2. **Recipient tracking**: All new committees are added to the `allRecipients` collection for tracking
3. **Contribution links**: Committees receive contributions from both individuals and companies

### What Gets Updated

When adding **individuals**:
- `constants/individuals` (individual metadata)
- `rawIndividualContributions` collection (individual's contribution data)
- `individuals` collection (processed individual data)
- `companies` collection (affected companies get updated `relatedIndividuals` and `contributions`)
- `allRecipients` collection (any new recipient committees discovered)

When adding **companies**:
- `constants/companies` (company metadata)
- `companies` collection (company data with `relatedIndividuals` and `contributions`)
- `allRecipients` collection (any new recipient committees discovered)

When adding **committees**:
- `constants/committees` (committee metadata)
- `committees` collection (full FEC data including financials)
- `totals/committees` (aggregate totals updated)
