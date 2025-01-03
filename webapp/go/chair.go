package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

var ErrNoChairs = fmt.Errorf("no chairs")

func getChairNotification(ctx context.Context, chair *Chair) (*chairGetNotificationResponseData, error) {
	nextData, newNotification := takeLatestUnsentNotificationResponseDataToChair(chair.ID)

	if nextData == nil {
		return nil, ErrNoChairs
	}

	if newNotification && nextData.Status == "COMPLETED" {
		tx, err := db.Beginx()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE chairs SET is_free = TRUE WHERE id = (SELECT chair_id FROM rides WHERE id = ?)`,
			nextData.RideID); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	if newNotification {
		slog.Info("notification sent", "chair", chair, "data", nextData)
	}

	return nextData, nil
}

func getChairNotificationChannel(ctx context.Context, chair *Chair) <-chan *chairGetNotificationResponseData {
	ch := make(chan *chairGetNotificationResponseData)
	go func() {
		for {
			var c chan *chairGetNotificationResponseData
			for {
				channelReady := false
				c, channelReady = getUnsentNotificationResponseDataToChairChannel(chair.ID)
				if channelReady {
					break
				}
				ch <- nil
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(chairRetryAfterMs) * time.Millisecond):
					continue
				}
			}

			for {
				select {
				case <-ctx.Done():
					return
				case data := <-c:
					ch <- data
				}
			}
		}
	}()
	return ch
}
