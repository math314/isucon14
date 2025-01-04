package main

import (
	"fmt"
)

var ErrNoChairs = fmt.Errorf("no chairs")

func getChairNotification(chair *Chair) (*chairGetNotificationResponseData, error) {
	nextData, newNotification := takeLatestUnsentNotificationResponseDataToChair(chair.ID)

	if nextData == nil {
		return nil, ErrNoChairs
	}

	if newNotification {
		return nextData, nil
	} else {
		return nil, nil
	}
}
