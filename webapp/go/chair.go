package main

import (
	"context"
	"fmt"
	"log/slog"
)

var ErrNoChairs = fmt.Errorf("no chairs")

func getChairNotification(ctx context.Context, chair *Chair) (*chairGetNotificationResponseData, error) {
	nextData, newNotification := takeLatestUnsentNotificationResponseDataToChair(chair.ID)

	tx, err := db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if nextData == nil {
		return nil, ErrNoChairs
	}

	if newNotification && nextData.Status == "COMPLETED" {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE chairs SET is_free = TRUE WHERE id = (SELECT chair_id FROM rides WHERE id = ?)`,
			nextData.RideID); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	if newNotification {
		slog.Info("notification sent", "chair", chair, "data", nextData)
	}

	return nextData, nil
}
