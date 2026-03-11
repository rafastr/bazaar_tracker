def rebuild_all(history_db, templates_db):
    history_db.rebuild_item_hero_wins()
    history_db.rebuild_item_firsts(templates_db)
    history_db.rebuild_achievements(templates_db)
