# Follow the Crypto - Daily Operations Setup

### Daily Operations (Most Common)

```bash
# Quick status check (30 seconds)
python daily.py status

# Quick daily update (15 minutes)
python daily.py quick

# Full daily update (30 minutes)
python daily.py full

# Add a new person to track
python daily.py add "Elon Musk"
```

### Advanced Daily Pipeline

```bash
# Full pipeline with custom time limit
python daily_pipeline.py --max-time 20

# Quick status check
python daily_pipeline.py --quick-check

# Skip certain operations
python daily_pipeline.py --skip expenditures --skip disbursements

# Verbose output with detailed logging
python daily_pipeline.py --verbose

# Save results to file
python daily_pipeline.py --output daily_results.json
```

### Individual Management

```bash
# Add individual with immediate fetch
python commands/add_individual.py --name "Warren Buffett" --fetch

# Add individual with specific committees
python commands/add_individual.py --name "Nancy Pelosi" --committees C00401224 C00575621

# Fetch data for existing individual
python commands/fetch_individual.py --name "Elon Musk"

# List all tracked individuals
python commands/list_individuals.py

# Search individuals
python commands/list_individuals.py --search "Tech"
```

### Batch Processing

```bash
# Process multiple individuals from file
python commands/batch_daily.py --file new_people.txt

# Process pending individuals (no recent data)
python commands/batch_daily.py --process-pending
```
