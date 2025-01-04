package main

import (
	"context"
	"fmt"
)

var ErrNoChairs = fmt.Errorf("no chairs")

func getChairNotification(ctx context.Context, chair *Chair) (*chairGetNotificationResponseData, error) {
	nextData, newNotification := takeLatestUnsentNotificationResponseDataToChair(chair.ID)

	if nextData == nil {
		return nil, ErrNoChairs
	}

	if newNotification && nextData.Status == "COMPLETED" {
		if _, err := db.ExecContext(ctx, `UPDATE chairs SET is_free = TRUE WHERE id = ?`, chair.ID); err != nil {
			return nil, err
		}
		if err := updateIsFreeInCache(chair.ID, true); err != nil {
			return nil, err
		}
	}

	// if newNotification {
	// 	slog.Info("notification sent", "chair", chair, "data", nextData)
	// }

	if newNotification {
		return nextData, nil
	} else {
		return nil, nil
	}
}
