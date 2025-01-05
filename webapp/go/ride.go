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
	EvaluationResultFlushed
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
	AppNotificationDone     bool
	ChairNotificationDone   bool
	EvaluationResultFlushed bool
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

	if rideStatusSentAtCache[request.RideStatusID] == nil {
		rideStatusSentAtCache[request.RideStatusID] = &RideStatusSentAt{}
	}
	rideStatusSentAt := rideStatusSentAtCache[request.RideStatusID]

	// rideStatus := RideStatus{}
	// if err := db.GetContext(ctx, &rideStatus, `SELECT * FROM ride_statuses WHERE id = ?`, request.RideStatusID); err != nil {
	// 	return err
	// }

	// if rideStatusSentAt.AppNotificationDone != (rideStatus.AppSentAt != nil) {
	// 	slog.Error("appSentAt is not consistent", "rideStatus", rideStatus, "rideStatusSentAt", rideStatusSentAt)
	// }
	// if rideStatusSentAt.ChairNotificationDone != (rideStatus.ChairSentAt != nil) {
	// 	slog.Error("chairSentAt is not consistent", "rideStatus", rideStatus, "rideStatusSentAt", rideStatusSentAt)
	// }

	// slog.Info("checkStatusAndUpdateChairFreeFlag", "rideStatus", rideStatus)

	if !rideStatusSentAt.AppNotificationDone || !rideStatusSentAt.ChairNotificationDone || !rideStatusSentAt.EvaluationResultFlushed {
		return errNoNeedToUpdate
	}

	slog.Info("checkStatusAndUpdateChairFreeFlag updating chairs to FREE", "chair", request.ChairID)
	if _, err := db.ExecContext(ctx, `UPDATE chairs SET is_free = 1 WHERE id = ?`, request.ChairID); err != nil {
	}
	if err := updateIsFreeInCache(request.ChairID, true); err != nil {
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

func updateRideStatusEvaluationResultFlushed(ctx context.Context, request RideStatusSentAtRequest) (time.Time, error) {
	time := time.Now()

	if rideStatusSentAtCache[request.RideStatusID] == nil {
		rideStatusSentAtCache[request.RideStatusID] = &RideStatusSentAt{}
	}
	rideStatusSentAtCache[request.RideStatusID].EvaluationResultFlushed = true
	slog.Info("updateRideStatusEvaluationResultFlushed", "rideStatusId", request.RideStatusID)

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
			} else if req.SentType == ChairNotification {
				if time, err := updateRideStatusChairSentAt(ctx, req); err != nil {
					slog.Error("failed to update chair sent at", "error", err)
				} else {
					slog.Info("updated chair sent at", "rideId", req.RideID, "chair_sent_at", time)
				}
			} else if req.SentType == EvaluationResultFlushed {
				if time, err := updateRideStatusEvaluationResultFlushed(ctx, req); err != nil {
					slog.Error("failed to update evaluation result flushed", "error", err)
				} else {
					slog.Info("updated evaluation result flushed", "rideId", req.RideID, "evaluation_result_flushed", time)
				}
			}
		}
	}()
}
