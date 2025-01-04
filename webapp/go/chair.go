package main

func getChairNotificationChannel(chairId string) <-chan *chairGetNotificationResponseData {
	unsentRideStatusesToChairRWMutex.Lock()
	defer unsentRideStatusesToChairRWMutex.Unlock()

	if _, ok := unsentRideStatusesToChairChan[chairId]; !ok {
		unsentRideStatusesToChairChan[chairId] = make(chan *chairGetNotificationResponseData, 10)
	}
	c := unsentRideStatusesToChairChan[chairId]

	return c
}
