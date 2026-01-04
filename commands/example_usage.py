#!/usr/bin/env python3
"""
Example usage of the individual management commands.

This demonstrates the new workflow for adding and managing individuals.
"""

import subprocess
import sys


def run_command(cmd, description):
    """Run a command and show the output."""
    print(f"\n{'='*60}")
    print(f"🔄 {description}")
    print(f"Command: {cmd}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        if result.returncode != 0:
            print(f"⚠️  Command failed with return code {result.returncode}")
        else:
            print("✅ Command completed successfully")
    except Exception as e:
        print(f"❌ Error running command: {e}")


def main():
    print("Individual Management Commands Example")
    print("=====================================")
    
    # Example 1: List existing individuals
    run_command(
        "python -m commands.list_individuals",
        "List all currently tracked individuals"
    )
    
    # Example 2: Add a new individual with immediate data fetching
    run_command(
        "python -m commands.add_individual --id 'example-person' --name 'Example Person' --zip '12345' --company 'Example Corp' --title 'CEO'",
        "Add new individual with immediate data fetching"
    )
    
    # Example 3: Add individual without immediate fetching
    run_command(
        "python -m commands.add_individual --id 'another-example' --name 'Another Example' --employer-search 'crypto,blockchain' --no-fetch",
        "Add individual without immediate data fetching"
    )
    
    # Example 4: Fetch data for the individual added without fetching
    run_command(
        "python -m commands.fetch_individual --id 'another-example'",
        "Fetch data for specific individual"
    )
    
    # Example 5: List individuals with verbose output
    run_command(
        "python -m commands.list_individuals --verbose",
        "List individuals with detailed information"
    )
    
    print(f"\n{'='*60}")
    print("📋 Summary of New Workflow")
    print(f"{'='*60}")
    print("""
Instead of:
1. Manually editing constants.individuals in Firestore
2. Running entire pipeline

You can now:
1. Add individual: python -m commands.add_individual --id 'person-name' --name 'Person Name' --zip '12345'
2. Check status: python -m commands.list_individuals
3. Fetch data: python -m commands.fetch_individual --id 'person-name'
4. Or run selective pipeline: python pipeline.py --tasks fetch_individual_spending_selective --individual-ids 'person-name'

Benefits:
- No manual Firestore editing
- Process individuals independently  
- See data status clearly
- Much faster for single additions
    """)


if __name__ == "__main__":
    main()