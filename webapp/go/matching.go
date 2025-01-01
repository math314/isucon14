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

	rides := []*Ride{}
	if err := tx.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at limit`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return
		}
		slog.Error("match error 1", "error", err)
		return
	}

	latestChairLocations := []ChairLocationLatest{}
	if err := tx.SelectContext(ctx, &latestChairLocations, "SELECT * FROM chair_locations_latest WHERE chair_id IN (SELECT id FROM chairs WHERE is_active = TRUE AND is_free = TRUE)"); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Info("no chairs")
			return
		}
	}

	if len(latestChairLocations) < 5 {
		// slog.Info("too few chairs")
		return
	}
	slog.Info("stats: ", "rides counts", len(rides), "latestChairLocations", len(latestChairLocations))

	selectedIdsSet := map[string]struct{}{}
	for _, ride := range rides {
		matchedId := ""
		nearest := 350

		for _, chair := range latestChairLocations {
			if _, ok := selectedIdsSet[chair.ChairID]; ok {
				continue
			}
			distance := abs(chair.Latitude-ride.PickupLatitude) + abs(chair.Longitude-ride.PickupLongitude)
			if distance < nearest {
				nearest = distance
				matchedId = chair.ChairID
			}
		}

		if matchedId == "" {
			slog.Info("no matched chair. Probably too far")
			continue
		}
		selectedIdsSet[matchedId] = struct{}{}

		if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matchedId, ride.ID); err != nil {
			slog.Error("failed to update ride", "error", err)
			return
		}

		if _, err := tx.ExecContext(ctx, `UPDATE chairs SET is_free = 0 WHERE id = ?`, matchedId); err != nil {
			slog.Error("failed to update chairs", "error", err)
			return
		}
	}

	// slog.Info("matched", "ride_id", selectedRide.ID, "chair_id", matchedId)
	tx.Commit()
}
