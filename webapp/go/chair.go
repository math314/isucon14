package main

import (
	"context"
	"fmt"
	"log/slog"
)

var ErrNoChairs = fmt.Errorf("no chairs")

func getChairNotification(ctx context.Context, chair *Chair) (*chairGetNotificationResponseData, error) {
	nextData, alreadySent := takeLatestUnsentNotificationResponseDataToChair(chair.ID)

	tx, err := db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if nextData == nil {
		return nil, ErrNoChairs
	}

	// if !alreadySent {
	// 	_, err := tx.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, yetSentRideStatus.ID)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	if !alreadySent && nextData.Status == "COMPLETED" {
		chairs := []*Chair{}
		if err := tx.SelectContext(ctx, &chairs, `SELECT * FROM chairs WHERE id in (SELECT chair_id FROM rides WHERE id = ?)`, nextData.RideID); err != nil {
			slog.Error("failed to get chairs", "error", err)
			return nil, err
		}
		if len(chairs) != 1 {
			slog.Error("too many chairs", "chairs", chairs)
		}
		chair = chairs[0]
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE chairs SET is_free = TRUE WHERE id = ?`,
			chair.ID); err != nil {
			return nil, err
		}
		slog.Info("chair is free", "chair_id", chair.ID)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	if !alreadySent {
		slog.Info("notification sent", "chair", chair, "data", nextData)
	}

	return nextData, nil
}
