# Bazaar Chronicle

Bazaar Chronicle is a local run tracker for **The Bazaar** that records your runs, analyzes performance, and tracks achievements and item mastery.

The application runs locally on your machine and stores all data in a SQLite database. No external services are required.

---

## Features

### Run tracking
- Record runs automatically from game logs
- Saves screenshots when run ends
- Manually add or edit runs
- OCR support for extracting run data

### Board tracking
- Record final board items
- Board editor

### Statistics dashboard
- Rank evolution graph
- Win/loss history
- Hero performance stats

### Achievements
- Collection of achievements

### Item mastery
Track progress toward:

- Using every item in a win
- Using item with other heroes in a win
- Ability to import your manual tem checklist from a csv.

### Fully local
- SQLite database
- Local image cache
- No external accounts required
- Works offline
- Export/import your data

---

## Screenshots

*(Add screenshots here later)*

Dashboard  
Runs  
Items  
Achievements  

---

## Installation

### Requirements

- Python 3.11+
- pip

### Clone repository

```bash
git clone https://github.com/rafastr/bazaar-chronicle
cd bazaar-chronicle
```

Install dependencies

```bash
pip install -r requirements.txt
```

Run the application


## Running the application
```bash
python -m web.app

```
Open in browser:
http://127.0.0.1:5000

## First setup

## Updating templates

## Backups
The Manage page allows:

Export run history
Export full tracker backup
Import JSON backups

## Data location
All data is stored locally in:
%APPDATA%\BazaarChronicles

## Project Goals
Bazaar Chronicles focuses on:
- deterministic rebuildable stats
- local-first data ownership
- achievement-based progression tracking
- clean and fast UI

### Next things to build
- Computer vision to read items that are not detected in the player log.
- Import item images directly from game files.

## License
MIT License

## Credits
The Bazaar is developed by Tempo Storm.
Bazaar Chronicles is a community tool and is not affiliated with Tempo Storm.
