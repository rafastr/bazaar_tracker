# Bazaar Chronicle
Local run tracker and achievement system for The Bazaar.

Bazaar Chronicle is a local run tracker for **The Bazaar**.
It records your runs, analyzes performance, and tracks achievements and item mastery.
The application runs locally on your machine as a small web app and opens in your browser.
- No accounts
- No cloud services
- All data stays on your computer.

---

## Features

### Run tracking
- Automatic run detection from game logs
- Screenshot capture at run end
- Manual run creation and editing
- OCR support for extracting run data

### Board tracking
- Record final board items
- Board editor

### Statistics dashboard
- Rank evolution graph
- Win/loss history
- Hero performance stats

### Achievements
- Achievement system based on run performance
- 25+ achievements to unlock

### Item mastery
Track progress toward:
- Using every item in a winning run
- Using items with different heroes in a winning run
- Import item checklist from external CSV

### Fully local
- SQLite database
- Local image cache
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

## Download and Run
Download the latest release from GitHub Releases.

Extract the archive and run:
`BazaarChronicle.exe`

Your browser will open automatically.
No installation required.
The tracker must be running while you play in order to record runs.

## Backups
Backups can be created from the Manage page.

Available options:
- Export run history
- Export full tracker backup
- Import JSON backups

## Data location
All data is stored locally in:
`%APPDATA%\Bazaar Chronicle`

This folder contains:
```
run_history.sqlite3
templates.sqlite3
assets/images/items
screenshots
logs
exports
```
You can back up your data by copying this folder.

### Import checklists from csv
- If you track your item completion in spreadsheet (for example the [PunNoFun](https://docs.google.com/spreadsheets/d/1ceJfc_7-J3tlwHwyo7V2XJ39TONwBMDBA_kPJR5hPjM/edit?gid=0#gid=0) spreadsheet), you can import it into Bazaar Chronicle. 
Export the spreadsheet to CSV, then import it using the Manage page.

## Development
Requirements:
`Python 3.11+`

Install dependencies:
`pip install -r requirements.txt`

Run the tracker:
`python bazaar_chronicle.py`

Build the executable:
`pyinstaller BazaarChronicle.spec`

## Next things to build
- Computer vision for detecting items not present in logs
- Importing item images directly from game files
- Mac support
- Integration with the Tempo launcher

## License
MIT License

## Credits
The Bazaar is developed by Tempo Storm.

Bazaar Chronicle is a community tool and is not affiliated with Tempo Storm.
