package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
			status, err = getLatestRideStatusFromCache(ride.ID)
			if err != nil {
				return appGetNotificationResponseData{}, err
			}
		} else {
			return appGetNotificationResponseData{}, err
		}
	} else {
		status = yetSentRideStatus.Status
	}

	fare, err := calculateDiscountedFare(ctx, tx, userID, ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
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
		chair := &Chair{}
		if err := tx.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, ride.ChairID); err != nil {
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
