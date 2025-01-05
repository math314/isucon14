package main

import (
	"context"
	"log/slog"
	"time"
)

type RideStatusSentType int

const (
	AppNotification RideStatusSentType = iota
	ChairNotification
)

type RideStatusSentAtRequest struct {
	RideID   string
	SentType RideStatusSentType
}

var rideStatusSentAtChan = make(chan RideStatusSentAtRequest, 1000)

func updateRideStatusAppSentAt(ctx context.Context, rideID string) (time.Time, error) {
	time := time.Now()
	_, err := db.ExecContext(ctx, `UPDATE ride_statuses SET app_sent_at = ? WHERE ride_id = ?`, time, rideID)

	// update cache as well

	return time, err
}

func updateRideStatusChairSentAt(ctx context.Context, rideID string) (time.Time, error) {
	time := time.Now()
	_, err := db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = ? WHERE ride_id = ?`, time, rideID)

	// update cache as well

	return time, err
}

func launchRideStatusSentAtSyncer() {
	go func() {
		for req := range rideStatusSentAtChan {
			ctx := context.Background()
			if req.SentType == AppNotification {
				if time, err := updateRideStatusAppSentAt(ctx, req.RideID); err != nil {
					slog.Error("failed to update app sent at", "error", err)
				} else {
					slog.Info("updated app sent at", "rideId", req.RideID, "app_sent_at", time)
				}
			} else {
				if time, err := updateRideStatusChairSentAt(ctx, req.RideID); err != nil {
					slog.Error("failed to update chair sent at", "error", err)
				} else {
					slog.Info("updated chair sent at", "rideId", req.RideID, "chair_sent_at", time)
				}
			}
		}
	}()
}
