package main

func getAppNotificationChannel(userId string) <-chan *appGetNotificationResponseData {
	unsentRideStatusesToAppRWMutex.Lock()
	defer unsentRideStatusesToAppRWMutex.Unlock()

	if _, ok := unsentRideStatusesToAppChan[userId]; !ok {
		unsentRideStatusesToAppChan[userId] = make(chan *appGetNotificationResponseData, 10)
	}
	c := unsentRideStatusesToAppChan[userId]

	return c
}
