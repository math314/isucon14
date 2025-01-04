package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var ErrNoRides = fmt.Errorf("no rides")

func getRideStatus(ctx context.Context, userID string) (appGetNotificationResponseData, error) {
	tx, err := db.Beginx()
	if err != nil {
		return appGetNotificationResponseData{}, err
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC LIMIT 1`, userID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return appGetNotificationResponseData{}, ErrNoRides
		}
		return appGetNotificationResponseData{}, err
	}

	yetSentRideStatus := RideStatus{}
	status := ""
	if err := tx.GetContext(ctx, &yetSentRideStatus, `SELECT * FROM ride_statuses WHERE ride_id = ? AND app_sent_at IS NULL ORDER BY created_at ASC LIMIT 1`, ride.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			status, err = getLatestRideStatus(ctx, tx, ride.ID)
			if err != nil {
				return appGetNotificationResponseData{}, err
			}
		} else {
			return appGetNotificationResponseData{}, err
		}
	} else {
		status = yetSentRideStatus.Status
	}

	fare, err := calculateDiscountedFare(ctx, tx, userID, ride.ID, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		return appGetNotificationResponseData{}, err
	}

	responseData := appGetNotificationResponseData{
		RideID: ride.ID,
		PickupCoordinate: Coordinate{
			Latitude:  ride.PickupLatitude,
			Longitude: ride.PickupLongitude,
		},
		DestinationCoordinate: Coordinate{
			Latitude:  ride.DestinationLatitude,
			Longitude: ride.DestinationLongitude,
		},
		Fare:      fare,
		Status:    status,
		CreatedAt: ride.CreatedAt.UnixMilli(),
		UpdateAt:  ride.UpdatedAt.UnixMilli(),
	}

	if ride.ChairID.Valid {
		chair, err := getChairByID(ride.ChairID.String)
		if err != nil {
			return appGetNotificationResponseData{}, err
		}

		stats, err := getChairStats(ctx, tx, chair.ID)
		if err != nil {
			return appGetNotificationResponseData{}, err
		}

		responseData.Chair = &appGetNotificationResponseChair{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
			Stats: stats,
		}
	}

	if yetSentRideStatus.ID != "" {
		_, err := tx.ExecContext(ctx, `UPDATE ride_statuses SET app_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, yetSentRideStatus.ID)
		if err != nil {
			return appGetNotificationResponseData{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return appGetNotificationResponseData{}, err
	}

	return responseData, nil
}

func takeLatestUnsentNotificationResponseDataToApp(userID string) (*appGetNotificationResponseData, bool) {
	unsentRideStatusesToAppRWMutex.Lock()
	defer unsentRideStatusesToAppRWMutex.Unlock()

	c, ok := unsentRideStatusesToAppChan[userID]
	if !ok {
		return nil, false
	}

	select {
	case data := <-c:
		sentLastRideStatusToApp[userID] = data
		return data, true
	default:
		return sentLastRideStatusToApp[userID], false
	}
}

func getRideStatusFromChannel(userID string) (*appGetNotificationResponseData, error) {
	nextData, newNotification := takeLatestUnsentNotificationResponseDataToApp(userID)

	if nextData == nil {
		return nil, ErrNoRides
	}

	if newNotification {
		slog.Info("getRideStatusFromChannel notification sent - ", "userId", userID, "data", nextData)
		return nextData, nil
	} else {
		return nil, nil
	}

}

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
	if err := tx.GetContext(ctx, &rideStatus, `SELECT * FROM ride_statuses WHERE ride_id = ?`, request.RideID); err != nil {
		return err
	}

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
