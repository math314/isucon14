package main

import (
	"context"
	"fmt"
	"log/slog"
)

var ErrNoChairs = fmt.Errorf("no chairs")

func getChairNotification(ctx context.Context, chair *Chair) (*chairGetNotificationResponseData, error) {
	nextData, newNotification := takeLatestUnsentNotificationResponseDataToChair(chair.ID)

	if nextData == nil {
		return nil, ErrNoChairs
	}

	if newNotification && nextData.Status == "COMPLETED" {
		if _, err := db.ExecContext(
			ctx,
			`UPDATE chairs SET is_free = TRUE WHERE id = (SELECT chair_id FROM rides WHERE id = ?)`,
			nextData.RideID); err != nil {
			return nil, err
		}
	}

	if newNotification {
		slog.Info("notification sent", "chair", chair, "data", nextData)
	}

	return nextData, nil
}
