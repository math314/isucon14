package main

import (
	"context"
	"errors"
	"log/slog"
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

type RideStatusSentAt struct {
	AppNotificationDone   bool
	ChairNotificationDone bool
}

var rideStatusSentAtCache = make(map[string]*RideStatusSentAt)

var errNoNeedToUpdate = errors.New("no need to update")

func checkStatusAndUpdateChairFreeFlag(ctx context.Context, request RideStatusSentAtRequest) error {
	if request.Status != "COMPLETED" {
		return errNoNeedToUpdate
	}
	if request.ChairID == "" {
		return errNoNeedToUpdate
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, ok := rideStatusSentAtCache[request.RideStatusID]; !ok {
		rideStatusSentAtCache[request.RideStatusID] = &RideStatusSentAt{}
	}
	rideStatusSentAt := rideStatusSentAtCache[request.RideStatusID]
	if !rideStatusSentAt.AppNotificationDone || !rideStatusSentAt.ChairNotificationDone {
		return errNoNeedToUpdate
	}

	// slog.Info("checkStatusAndUpdateChairFreeFlag updating chairs to FREE", "chair", request.ChairID)
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

func updateRideStatusAppSentAt(ctx context.Context, request RideStatusSentAtRequest) error {
	// if _, err := db.ExecContext(ctx, `UPDATE ride_statuses SET app_sent_at = ? WHERE id = ?`, time, request.RideStatusID); err != nil {
	// 	return time, err
	// }

	if _, ok := rideStatusSentAtCache[request.RideStatusID]; !ok {
		rideStatusSentAtCache[request.RideStatusID] = &RideStatusSentAt{}
	}
	rideStatusSentAtCache[request.RideStatusID].AppNotificationDone = true

	if err := checkStatusAndUpdateChairFreeFlag(ctx, request); err != nil {
		if errors.Is(err, errNoNeedToUpdate) {
			return nil
		} else {
			return err
		}
	}

	return nil
}

func updateRideStatusChairSentAt(ctx context.Context, request RideStatusSentAtRequest) error {
	// time := time.Now()
	// if _, err := db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = ? WHERE id = ?`, time, request.RideStatusID); err != nil {
	// 	return time, err
	// }
	// slog.Info("updateRideStatusChairSentAt", "rideStatusId", request.RideStatusID, "time", time)

	if _, ok := rideStatusSentAtCache[request.RideStatusID]; !ok {
		rideStatusSentAtCache[request.RideStatusID] = &RideStatusSentAt{}
	}
	rideStatusSentAtCache[request.RideStatusID].ChairNotificationDone = true

	if err := checkStatusAndUpdateChairFreeFlag(ctx, request); err != nil {
		if errors.Is(err, errNoNeedToUpdate) {
			return nil
		} else {
			return err
		}
	}

	return nil
}

func launchRideStatusSentAtSyncer() {
	go func() {
		for req := range rideStatusSentAtChan {
			ctx := context.Background()
			if req.SentType == AppNotification {
				if err := updateRideStatusAppSentAt(ctx, req); err != nil {
					slog.Error("failed to update app sent at", "error", err)
				} else {
					// slog.Info("updated app sent at", "rideId", req.RideID, "app_sent_at", time)
				}
			} else {
				if err := updateRideStatusChairSentAt(ctx, req); err != nil {
					slog.Error("failed to update chair sent at", "error", err)
				} else {
					// slog.Info("updated chair sent at", "rideId", req.RideID, "chair_sent_at", time)
				}
			}
		}
	}()
}
