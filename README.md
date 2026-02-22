# Bazaar Tracker

A run tracker for **The Bazaar** that logs run history and end-game setups automatically. 

My goal is to have tool that records items used on end game runs. Then to have an achievment system.
It's inspired by the google docs checklist I see the youtuber pun pun no fun using to manually record his runs.
https://docs.google.com/spreadsheets/d/1ceJfc_7-J3tlwHwyo7V2XJ39TONwBMDBA_kPJR5hPjM/edit?gid=277039937#gid=277039937

> Status: early development (MVP in progress)

## Features (Current)
- [x] Detect end-of-run via `Player.log` trigger
- [x] Auto-capture end screen screenshot
- [ ] Store runs in SQLite
- [ ] (Planned) Parse logs to capture items without CV

## Roadmap
### MVP
- [x] Windows log watcher triggers capture
- [x] Save screenshot with timestamp
- [ ] Minimal run record in SQLite

### Next
- [ ] Log parsing: detect items/skills from game logs (if available)
- [ ] Manual correction flow for unknown items
- [ ] Basic stats: runs, wins, most used items

### Later
- [ ] Achievements system
- [ ] GUI (desktop or local web UI)
- [ ] Auto-update item/skill database

## Disclaimer:
- This is an unofficial project and is not affiliated with Tempo Storm.
- Contributions are welcome.

