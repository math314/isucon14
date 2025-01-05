package main

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type RideStatusSentType int

const (
	AppNotification RideStatusSentType = iota
	ChairNotification
)

type RideStatusSentAtRequest struct {
	RideStatusID string
	RideID       string
	ChairID      string
	Status       string
	SentType     RideStatusSentType
}

var rideStatusSentAtChan = make(chan RideStatusSentAtRequest, 1000)

func checkStatusAndUpdateChairFreeFlag(ctx context.Context, request RideStatusSentAtRequest) error {
	if request.Status != "COMPLETED" {
		return nil
	}
	if request.ChairID == "" {
		return nil
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rideStatus := RideStatus{}
	if err := tx.GetContext(ctx, &rideStatus, `SELECT * FROM ride_statuses WHERE id = ?`, request.RideStatusID); err != nil {
		return err
	}
	slog.Info("checkStatusAndUpdateChairFreeFlag", "rideStatus", rideStatus)

	if rideStatus.AppSentAt == nil || rideStatus.ChairSentAt == nil {
		return errors.New("app_sent_at or chair_sent_at is nil")
	}

	slog.Info("checkStatusAndUpdateChairFreeFlag updating chairs to FREE", "chair", request.ChairID)
	if _, err := tx.ExecContext(ctx, `UPDATE chairs SET is_free = 1 WHERE id = ?`, request.ChairID); err != nil {
		return err
	}
	if err := updateIsFreeInCache(request.ChairID, true); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func updateRideStatusAppSentAt(ctx context.Context, request RideStatusSentAtRequest) (time.Time, error) {
	time := time.Now()
	if _, err := db.ExecContext(ctx, `UPDATE ride_statuses SET app_sent_at = ? WHERE id = ?`, time, request.RideStatusID); err != nil {
		return time, err
	}
	slog.Info("updateRideStatusAppSentAt", "rideStatusId", request.RideStatusID, "time", time)

	if err := checkStatusAndUpdateChairFreeFlag(ctx, request); err != nil {
		return time, err
	}
	// update cache as well

	return time, nil
}

func updateRideStatusChairSentAt(ctx context.Context, request RideStatusSentAtRequest) (time.Time, error) {
	time := time.Now()
	if _, err := db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = ? WHERE id = ?`, time, request.RideStatusID); err != nil {
		return time, err
	}
	slog.Info("updateRideStatusChairSentAt", "rideStatusId", request.RideStatusID, "time", time)

	if err := checkStatusAndUpdateChairFreeFlag(ctx, request); err != nil {
		return time, err
	}
	// update cache as well

	return time, nil
}

func launchRideStatusSentAtSyncer() {
	go func() {
		for req := range rideStatusSentAtChan {
			ctx := context.Background()
			if req.SentType == AppNotification {
				if time, err := updateRideStatusAppSentAt(ctx, req); err != nil {
					slog.Error("failed to update app sent at", "error", err)
				} else {
					slog.Info("updated app sent at", "rideId", req.RideID, "app_sent_at", time)
				}
			} else {
				if time, err := updateRideStatusChairSentAt(ctx, req); err != nil {
					slog.Error("failed to update chair sent at", "error", err)
				} else {
					slog.Info("updated chair sent at", "rideId", req.RideID, "chair_sent_at", time)
				}
			}
		}
	}()
}
