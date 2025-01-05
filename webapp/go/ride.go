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

	// rideStatus := RideStatus{}
	// if err := tx.GetContext(ctx, &rideStatus, `SELECT * FROM ride_statuses WHERE id = ?`, request.RideStatusID); err != nil {
	// 	return err
	// }
	if rideStatusSentAtCache[request.RideStatusID] == nil {
		rideStatusSentAtCache[request.RideStatusID] = &RideStatusSentAt{}
	}
	rideStatusSentAt := rideStatusSentAtCache[request.RideStatusID]

	// if rideStatusSentAt.AppNotificationDone != (rideStatus.AppSentAt != nil) {
	// 	slog.Error("appSentAt is not consistent", "rideStatus", rideStatus, "rideStatusSentAt", rideStatusSentAt)
	// }
	// if rideStatusSentAt.ChairNotificationDone != (rideStatus.ChairSentAt != nil) {
	// 	slog.Error("chairSentAt is not consistent", "rideStatus", rideStatus, "rideStatusSentAt", rideStatusSentAt)
	// }

	// slog.Info("checkStatusAndUpdateChairFreeFlag", "rideStatus", rideStatus)

	if !rideStatusSentAt.AppNotificationDone || !rideStatusSentAt.ChairNotificationDone {
		return errNoNeedToUpdate
	}
	time.Sleep(10 * time.Millisecond)

	// slog.Info("checkStatusAndUpdateChairFreeFlag updating chairs to FREE", "chair", request.ChairID)
	if _, err := db.ExecContext(ctx, `UPDATE chairs SET is_free = 1 WHERE id = ?`, request.ChairID); err != nil {
		return err
	}
	if err := updateIsFreeInCache(request.ChairID, true); err != nil {
		return err
	}

	return nil
}

func updateRideStatusAppSentAt(ctx context.Context, request RideStatusSentAtRequest) (time.Time, error) {
	time := time.Now()
	// if _, err := db.ExecContext(ctx, `UPDATE ride_statuses SET app_sent_at = ? WHERE id = ?`, time, request.RideStatusID); err != nil {
	// 	return time, err
	// }
	if rideStatusSentAtCache[request.RideStatusID] == nil {
		rideStatusSentAtCache[request.RideStatusID] = &RideStatusSentAt{}
	}
	rideStatusSentAtCache[request.RideStatusID].AppNotificationDone = true
	slog.Info("updateRideStatusAppSentAt", "rideStatusId", request.RideStatusID, "time", time)

	if err := checkStatusAndUpdateChairFreeFlag(ctx, request); err != nil {
		if errors.Is(err, errNoNeedToUpdate) {
			return time, nil
		} else {
			return time, err
		}
	}

	return time, nil
}

func updateRideStatusChairSentAt(ctx context.Context, request RideStatusSentAtRequest) (time.Time, error) {
	time := time.Now()
	// if _, err := db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = ? WHERE id = ?`, time, request.RideStatusID); err != nil {
	// 	return time, err
	// }
	if rideStatusSentAtCache[request.RideStatusID] == nil {
		rideStatusSentAtCache[request.RideStatusID] = &RideStatusSentAt{}
	}
	rideStatusSentAtCache[request.RideStatusID].ChairNotificationDone = true
	slog.Info("updateRideStatusChairSentAt", "rideStatusId", request.RideStatusID, "time", time)

	if err := checkStatusAndUpdateChairFreeFlag(ctx, request); err != nil {
		if errors.Is(err, errNoNeedToUpdate) {
			return time, nil
		} else {
			return time, err
		}
	}

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
