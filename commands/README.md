# Data Management Commands

Command-line utilities to manage individuals, companies, and committees without manually editing Firestore.

## Dependency Tracking

The system automatically handles data dependencies:

**Individual → Company**: Adding an individual with a company field updates that company's `relatedIndividuals` list and contribution totals.

**Company → Individual**: Adding a company aggregates contributions from linked individuals.

**Committee**: Committees track contributions from individuals/companies and fetch FEC data when hydrated.

## Individual Commands

### Add Individual
```bash
python -m commands.add_individual --id "john-doe" --name "John Doe" --zip "12345"
python -m commands.add_individual --id "jane-smith" --company "Coinbase" --title "CEO"
python -m commands.add_individual --id "bob-jones" --employer-search "crypto,blockchain" --no-fetch
```

Options: `--id` (required), `--name`, `--name-search`, `--zip`, `--city`, `--employer-search`, `--company`, `--title`, `--photo-credit`, `--no-fetch`

### Fetch Individual Data
```bash
python -m commands.fetch_individual --id "john-doe"
python -m commands.fetch_individual --id "jane-smith" --force
```

### List Individuals
```bash
python -m commands.list_individuals
python -m commands.list_individuals --verbose
```

### Batch Operations
```bash
python -m commands.batch_daily --add "person1:Company A" --add "person2:Company B"
python -m commands.batch_daily --process-pending
```

## Company Commands

### Add Company
```bash
python -m commands.add_company --id "example-corp" --name "Example Corp" --category "exchange"
python -m commands.add_company --id "crypto-ventures" --name "Crypto Ventures" --category "capital" --category "exchange"
python -m commands.add_company --id "blockchain-inc" --name "Blockchain Inc" --category "other" --description "A blockchain company" --no-fetch
```

Options: `--id` (required), `--name` (required), `--category` (required, repeatable), `--search-id`, `--description`, `--country`, `--no-fetch`

### List Companies
```bash
python -m commands.list_companies
python -m commands.list_companies --verbose
```

## Committee Commands

### Add Committee
```bash
python -m commands.add_committee --id "C00123456" --name "Example PAC" --description "A PAC focused on crypto policy"
python -m commands.add_committee --id "C00654321" --name "Tech Freedom Fund" --no-fetch
```

Options: `--id` (required, format C########), `--name` (required), `--description`, `--no-fetch`

### List Committees
```bash
python -m commands.list_committees
python -m commands.list_committees --verbose
```

## Field Reference

**Individual**: `id`, `name`, `nameSearch`, `zip`, `city`, `company` (array), `employerSearch` (array), `title`, `photoCredit`

**Company**: `id`, `name`, `search_id`, `category` (array), `description`, `country`

**Committee**: `id` (C########), `name`, `description`

## What Gets Updated

**Adding individuals:**
- `constants/individuals`
- `rawIndividualContributions` collection
- `individuals` collection
- `companies` collection (affected companies)
- `allRecipients` collection (new recipients)

**Adding companies:**
- `constants/companies`
- `companies` collection
- `allRecipients` collection (new recipients)

**Adding committees:**
- `constants/committees`
- `committees` collection (with FEC data)
- `totals/committees`

## Dependencies

- Core: Database and pipeline infrastructure
- Optional: `tabulate` for better table formatting
