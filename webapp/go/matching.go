package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
)

var chairIdToLatestRideIdMutex = &sync.RWMutex{}
var chairIdToLatestRideId = make(map[string]*Ride)

func loadLatestRideToChairAssignments() error {
	rides := []*Ride{}
	if err := db.Select(&rides, "SELECT * FROM rides WHERE chair_id IS NOT NULL ORDER BY updated_at DESC"); err != nil {
		slog.Error("loadLatestRideToChairAssignments", "error", err)
		return err
	}

	chairIdToLatestRideIdMutex.Lock()
	defer chairIdToLatestRideIdMutex.Unlock()

	chairIdToLatestRideId = make(map[string]*Ride)
	for _, ride := range rides {
		if _, ok := chairIdToLatestRideId[ride.ChairID.String]; ok {
			continue
		}
		chairIdToLatestRideId[ride.ChairID.String] = ride
	}
	return nil
}

func assignRideToChair(chairId string, ride Ride) {
	chairIdToLatestRideIdMutex.Lock()
	defer chairIdToLatestRideIdMutex.Unlock()

	chairIdToLatestRideId[chairId] = &ride
}

func getLatestRideByChairId(chairId string) (*Ride, bool) {
	chairIdToLatestRideIdMutex.RLock()
	defer chairIdToLatestRideIdMutex.RUnlock()
	ride, ok := chairIdToLatestRideId[chairId]

	return ride, ok
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

	slog.Info("runMatching started")

	latestChairLocations := []ChairLocationLatest{}
	chairCacheMapRWMutex.RLock()
	chairLocationCacheMapRWMutex.RLock()
	for _, chair := range chairCacheMap {
		if !chair.IsActive {
			continue
		}
		latestSentStatus, found := getChairIdToSentLatestRideStatus(chair.ID)
		if found && latestSentStatus != "COMPLETED" {
			continue
		}
		loc, ok := chairLocationCacheMap[chair.ID]
		if !ok {
			continue
		}

		latestChairLocations = append(latestChairLocations, ChairLocationLatest{
			ChairID:       chair.ID,
			Latitude:      loc.Latitude,
			Longitude:     loc.Longitude,
			TotalDistance: loc.TotalDistance,
		})
	}
	chairCacheMapRWMutex.RUnlock()
	chairLocationCacheMapRWMutex.RUnlock()

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

	if err := updateIsFreeInCache(matchedId, false); err != nil {
		slog.Error("failed to update is free in cache", "error", err)
		return
	}

	newRide := Ride{
		ID:                   ride.ID,
		UserID:               ride.UserID,
		ChairID:              sql.NullString{String: matchedId, Valid: true},
		PickupLatitude:       ride.PickupLatitude,
		PickupLongitude:      ride.PickupLongitude,
		DestinationLatitude:  ride.DestinationLatitude,
		DestinationLongitude: ride.DestinationLongitude,
		Evaluation:           ride.Evaluation,
		CreatedAt:            ride.CreatedAt,
		UpdatedAt:            ride.UpdatedAt, // not need to update "updatedAt"
	}

	slog.Info("matched", "chair_id", matchedId, "ride_id", ride.ID)
	assignRideToChair(matchedId, newRide)

	chairId, err := buildAndAppendChairGetNotificationResponseData(ctx, tx, ride.ID, "MATCHING")
	if err != nil {
		slog.Error("failed to build and append chair get notification response data", "error", err)
		return
	}
	if err := buildAndAppendAppGetNotificationResponseData(ctx, tx, ride.ID, "MATCHING"); err != nil {
		slog.Error("failed to build and append app get notification response data", "error", err)
		return
	}
	latestRideStatusCacheMapRWMutex.Lock()
	defer latestRideStatusCacheMapRWMutex.Unlock()

	if _, ok := chairIdToUnsentRideStatusesMap[chairId]; !ok {
		chairIdToUnsentRideStatusesMap[chairId] = make(map[UnsentRideStatusKey]int)
	}
	chairIdToUnsentRideStatusesMap[chairId][UnsentRideStatusKey{
		rideId: ride.ID,
		status: "MATCHING",
	}] = 0

	if err := tx.Commit(); err != nil {
		slog.Error("failed to commit tx", "error", err)
		return
	}

	slog.Info("runMatching finished")
}
