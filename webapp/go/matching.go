package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
)

func runMatching() {
	ctx := context.Background()

	tx, err := db.Beginx()
	if err != nil {
		slog.Error("failed to begin tx", "error", err)
		return
	}
	defer tx.Rollback()

	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Info("no rides")
			return
		}
		slog.Error("match error 1", "error", err)
		return
	}

	latestChairLocations := []ChairLocationLatest{}
	if err := tx.SelectContext(ctx, latestChairLocations, "SELECT * FROM chair_locations_latest WHERE chair_id IN (SELECT id FROM chairs WHERE is_active = TRUE AND is_free = TRUE)"); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Info("no chairs")
			return
		}
	}

	if len(latestChairLocations) == 0 {
		slog.Info("not empty")
		return
	}

	// nearest chair
	matchedId := ""
	nearest := 10000000
	for _, chair := range latestChairLocations {
		distance := abs(chair.Latitude-ride.PickupLatitude) + abs(chair.Longitude-ride.PickupLongitude)
		if distance < nearest {
			nearest = distance
			matchedId = chair.ChairID
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE chairs SET is_free = 0 WHERE id = ?`, matchedId); err != nil {
		slog.Error("failed to update chairs", "error", err)
		return
	}
	slog.Info("matched", "ride_id", ride.ID, "chair_id", matchedId)
	tx.Commit()
}
