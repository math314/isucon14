package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"
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
	rides := []*Ride{}
	if err := tx.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 20`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return
		}
		slog.Error("match error 1", "error", err)
		return
	}

	latestChairLocations := []ChairLocationLatest{}
	chairCacheMapRWMutex.RLock()
	chairLocationCacheMapRWMutex.RLock()
	for _, chair := range chairCacheMap {
		if !chair.IsActive || !chair.IsFree {
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

	if len(rides) == 0 || len(latestChairLocations) < 5 {
		return
	}

	slog.Info("runMatching started", "rides", len(rides), "chairs", len(latestChairLocations))
	usedChairs := make(map[string]struct{})
	for _, ride := range rides {
		// nearest chair
		matchedId := ""
		nearest := 10000000
		for _, chair := range latestChairLocations {
			if _, ok := usedChairs[chair.ChairID]; ok {
				continue
			}
			distance := abs(chair.Latitude-ride.PickupLatitude) + abs(chair.Longitude-ride.PickupLongitude)
			if distance < nearest {
				nearest = distance
				matchedId = chair.ChairID
			}
		}
		if matchedId == "" {
			slog.Info("no chairs left")
			break
		}
		usedChairs[matchedId] = struct{}{}

		now := time.Now().Truncate(time.Microsecond)
		if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ?, updated_at = ? WHERE id = ?", matchedId, now, ride.ID); err != nil {
			slog.Error("failed to update ride", "error", err)
			return
		}
		if err := updateRideChairIdInCache(ride.ID, matchedId, now); err != nil {
			slog.Error("failed to update ride chair id", "error", err)
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

		rideStatus := &RideStatus{}
		if err := tx.GetContext(ctx, rideStatus, `SELECT * FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1`, ride.ID); err != nil {
			slog.Error("failed to get ride status", "error", err)
			return
		}
		if rideStatus.Status != "MATCHING" {
			slog.Error("invalid ride status", "rideStatus", rideStatus)
			return
		}

		if _, err := buildAndAppendChairGetNotificationResponseData(ctx, tx, rideStatus.ID, ride.ID, "MATCHING"); err != nil {
			slog.Error("failed to build and append chair get notification response data", "error", err)
			return
		}
		if _, err := buildAndAppendAppGetNotificationResponseData(ctx, tx, rideStatus.ID, ride.ID, "MATCHING"); err != nil {
			slog.Error("failed to build and append app get notification response data", "error", err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		slog.Error("failed to commit tx", "error", err)
		return
	}

	slog.Info("runMatching finished")
}
