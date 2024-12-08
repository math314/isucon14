package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
)

func runMatching() {
	ctx := context.Background()
	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	ride := &Ride{}

	tx, err := db.Begin()
	if err != nil {
		slog.Error("failed to begin tx", "error", err)
		return
	}
	defer tx.Rollback()

	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Info("no rides")
			return
		}
		slog.Error("match error 1", "error", err)
		return
	}

	matched := &Chair{}
	empty := false
	for i := 0; i < 10; i++ {
		if err := db.GetContext(ctx, matched, "SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1"); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				slog.Error("no chairs")
				return
			}
		}

		if err := db.GetContext(ctx, &empty, "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE", matched.ID); err != nil {
			slog.Error("match error 2", "error", err)
			return
		}
		if empty {
			slog.Info("empty")
			break
		}
	}
	if !empty {
		slog.Info("not empty")
		return
	}

	if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
		slog.Error("failed to update ride", "error", err)
		return
	}
	tx.Commit()
}
