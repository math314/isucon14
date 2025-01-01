package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
)

var chairIdToLatestRideIdMutex = &sync.RWMutex{}
var chairIdToLatestRideId = make(map[string]string)

func loadLatestRideToChairAssignments() error {
	rides := []*Ride{}
	if err := db.Select(&rides, "SELECT * FROM rides WHERE chair_id IS NOT NULL ORDER BY updated_at DESC"); err != nil {
		slog.Error("loadLatestRideToChairAssignments", "error", err)
		return err
	}

	chairIdToLatestRideIdMutex.Lock()
	defer chairIdToLatestRideIdMutex.Unlock()

	chairIdToLatestRideId = make(map[string]string)
	for _, ride := range rides {
		if _, ok := chairIdToLatestRideId[ride.ChairID.String]; ok {
			continue
		}
		chairIdToLatestRideId[ride.ChairID.String] = ride.ID
	}
	return nil
}

func assignRideToChair(chairId, rideId string) {
	chairIdToLatestRideIdMutex.Lock()
	defer chairIdToLatestRideIdMutex.Unlock()
	slog.Info("chairIdToLatestRideId[chair_id] = ride_id: ", "chair_id", chairId, "ride_id", rideId)
	chairIdToLatestRideId[chairId] = rideId
}

func getLatestRideIdByChairId(chairId string) (string, bool) {
	chairIdToLatestRideIdMutex.RLock()
	defer chairIdToLatestRideIdMutex.RUnlock()
	rideId, ok := chairIdToLatestRideId[chairId]
	return rideId, ok
}

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
	if matchedId == "" {
		slog.Error("no matched chair")
		return
	}

	if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matchedId, ride.ID); err != nil {
		slog.Error("failed to update ride", "error", err)
		return
	}

	if _, err := tx.ExecContext(ctx, `UPDATE chairs SET is_free = 0 WHERE id = ?`, matchedId); err != nil {
		slog.Error("failed to update chairs", "error", err)
		return
	}

	slog.Info("matched", "chair_id", matchedId, "ride_id", ride.ID)
	assignRideToChair(matchedId, ride.ID)
	tx.Commit()
}
