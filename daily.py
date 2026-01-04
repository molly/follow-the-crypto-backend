#!/usr/bin/env python3
"""
Quick daily operations script for Follow the Crypto.

This is the main entry point for daily operations. It provides simple commands
for the most common daily tasks.
"""

import sys
import subprocess
import json
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return the result."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} completed")
            return True, result.stdout
        else:
            print(f"❌ {description} failed: {result.stderr}")
            return False, result.stderr
    except Exception as e:
        print(f"❌ {description} error: {e}")
        return False, str(e)


def daily_quick():
    """Run a quick daily update (15 minutes)."""
    print("🚀 Running quick daily update...")
    return run_command("python daily_pipeline.py --max-time 15", "Quick daily pipeline")


def daily_full():
    """Run a full daily update (30 minutes)."""
    print("🚀 Running full daily update...")
    return run_command("python daily_pipeline.py --max-time 30", "Full daily pipeline")


def daily_status():
    """Check what needs updating without making changes."""
    print("🔍 Checking daily status...")
    success, output = run_command("python daily_pipeline.py --quick-check", "Status check")
    return success, output


def add_person(name, committees=None):
    """Add a new person to track."""
    cmd = f"python commands/add_individual.py --name '{name}'"
    if committees:
        cmd += f" --committees {' '.join(committees)}"
    cmd += " --fetch"  # Immediately fetch their data
    
    return run_command(cmd, f"Adding {name}")


def add_multiple_people(people_file):
    """Add multiple people from a file."""
    if not Path(people_file).exists():
        return False, f"File {people_file} not found"
    
    return run_command(f"python commands/batch_daily.py --file {people_file}", "Batch adding people")


def main():
    if len(sys.argv) < 2:
        print("""
Follow the Crypto - Daily Operations

Usage:
    python daily.py status          # Check what needs updating
    python daily.py quick           # Quick update (15 min)
    python daily.py full            # Full update (30 min)
    python daily.py add "Name"      # Add new person to track
    python daily.py batch file.txt  # Add multiple people
    
Examples:
    python daily.py status
    python daily.py quick
    python daily.py add "Elon Musk"
    python daily.py batch new_people.txt
        """)
        return 1
    
    command = sys.argv[1].lower()
    
    if command == "status":
        success, output = daily_status()
        print(output)
        return 0 if success else 1
        
    elif command == "quick":
        success, output = daily_quick()
        return 0 if success else 1
        
    elif command == "full":
        success, output = daily_full()
        return 0 if success else 1
        
    elif command == "add":
        if len(sys.argv) < 3:
            print("❌ Please provide a name: python daily.py add \"Name\"")
            return 1
        name = sys.argv[2]
        success, output = add_person(name)
        print(output)
        return 0 if success else 1
        
    elif command == "batch":
        if len(sys.argv) < 3:
            print("❌ Please provide a file: python daily.py batch file.txt")
            return 1
        filename = sys.argv[2]
        success, output = add_multiple_people(filename)
        print(output)
        return 0 if success else 1
        
    else:
        print(f"❌ Unknown command: {command}")
        print("Use 'python daily.py' to see available commands")
        return 1


if __name__ == "__main__":
    exit(main())