package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
)

type chairPostChairsRequest struct {
	Name               string `json:"name"`
	Model              string `json:"model"`
	ChairRegisterToken string `json:"chair_register_token"`
}

type chairPostChairsResponse struct {
	ID      string `json:"id"`
	OwnerID string `json:"owner_id"`
}

var chairLocationCacheMapRWMutex = sync.RWMutex{}
var chairLocationCacheMap map[string]*ChairLocationLatest = make(map[string]*ChairLocationLatest)

var chairCacheMapRWMutex = sync.RWMutex{}
var chairCacheMap map[string]*Chair = make(map[string]*Chair)
var accessTokenToChairCacheMap map[string]*Chair = make(map[string]*Chair)

func loadChairCacheMap() error {
	chairCacheMapRWMutex.Lock()
	defer chairCacheMapRWMutex.Unlock()

	chairs := []*Chair{}
	if err := db.Select(&chairs, "SELECT * FROM chairs"); err != nil {
		return err
	}

	chairCacheMap = make(map[string]*Chair)
	for _, chair := range chairs {
		chairCacheMap[chair.ID] = chair
	}
	accessTokenToChairCacheMap = make(map[string]*Chair)
	for _, chair := range chairs {
		accessTokenToChairCacheMap[chair.AccessToken] = chair
	}
	return nil
}

func getChairByAccessToken(accessToken string) (*Chair, error) {
	chairCacheMapRWMutex.RLock()
	defer chairCacheMapRWMutex.RUnlock()

	chair, ok := accessTokenToChairCacheMap[accessToken]
	if !ok {
		return nil, ErrNoChairs
	}

	return chair, nil
}

func getChairByID(chairID string) (*Chair, error) {
	chairCacheMapRWMutex.RLock()
	defer chairCacheMapRWMutex.RUnlock()

	chair, ok := chairCacheMap[chairID]
	if !ok {
		return nil, ErrNoChairs
	}

	return chair, nil
}

func insertChairCacheMap(chair Chair) {
	chairCacheMapRWMutex.Lock()
	defer chairCacheMapRWMutex.Unlock()

	chairCacheMap[chair.ID] = &chair
	accessTokenToChairCacheMap[chair.AccessToken] = &chair
}

func updateIsActiveInCache(chairId string, isActive bool) error {

	chairCacheMapRWMutex.Lock()
	defer chairCacheMapRWMutex.Unlock()

	chair, ok := chairCacheMap[chairId]
	if !ok {
		return errors.New("chair not found")
	}

	chair.IsActive = isActive
	return nil
}

func updateIsFreeInCache(chairId string, isFree bool) error {
	chairCacheMapRWMutex.Lock()
	defer chairCacheMapRWMutex.Unlock()

	chair, ok := chairCacheMap[chairId]
	if !ok {
		return errors.New("chair not found")
	}

	chair.IsFree = isFree
	return nil
}

var latestRideStatusCacheMapRWMutex = sync.RWMutex{}
var latestRideStatusCacheMap map[string]*RideStatus = make(map[string]*RideStatus)

func loadLatestRideStatusCacheMap() error {
	latestRideStatusCacheMapRWMutex.Lock()
	defer latestRideStatusCacheMapRWMutex.Unlock()

	rideStatuses := []*RideStatus{}
	if err := db.Select(&rideStatuses, "SELECT * FROM ride_statuses ORDER BY created_at DESC"); err != nil {
		slog.Error("loadLatestRideStatusCacheMap", "error", err)
		return err
	}

	latestRideStatusCacheMap = make(map[string]*RideStatus)

	for _, rideStatus := range rideStatuses {
		if _, ok := latestRideStatusCacheMap[rideStatus.RideID]; !ok {
			latestRideStatusCacheMap[rideStatus.RideID] = rideStatus
		}
	}
	return nil
}

var rideCacheMapRWMutex = sync.RWMutex{}
var rideCacheMap map[string]*Ride = make(map[string]*Ride)

type TotalCountAndTotalEvaluation struct {
	TotalCount         int
	TotalEvaluationSum int
}

var rideCachePerChairAndHasEvaluation map[string]TotalCountAndTotalEvaluation = make(map[string]TotalCountAndTotalEvaluation)

func updateRideCachePerChairAndHasEvaluationIfNeeded(ride *Ride) {
	if ride.ChairID.Valid && ride.Evaluation != nil {
		currentTotal := rideCachePerChairAndHasEvaluation[ride.ChairID.String]
		currentTotal.TotalCount++
		currentTotal.TotalEvaluationSum += *ride.Evaluation
		rideCachePerChairAndHasEvaluation[ride.ChairID.String] = currentTotal
	}
}

func getRideCachePerChairAndHasEvaluation(chairID string) TotalCountAndTotalEvaluation {
	rideCacheMapRWMutex.RLock()
	defer rideCacheMapRWMutex.RUnlock()

	return rideCachePerChairAndHasEvaluation[chairID]
}

func loadRideCacheMap() error {
	rideCacheMapRWMutex.Lock()
	defer rideCacheMapRWMutex.Unlock()

	rides := []*Ride{}
	if err := db.Select(&rides, "SELECT * FROM rides"); err != nil {
		return err
	}

	rideCacheMap = make(map[string]*Ride)
	rideCachePerChairAndHasEvaluation = make(map[string]TotalCountAndTotalEvaluation)
	for _, ride := range rides {
		rideCacheMap[ride.ID] = ride
		updateRideCachePerChairAndHasEvaluationIfNeeded(ride)
	}
	return nil
}

func insertRideCacheMap(ride Ride) {
	rideCacheMapRWMutex.Lock()
	defer rideCacheMapRWMutex.Unlock()

	rideCacheMap[ride.ID] = &ride
	updateRideCachePerChairAndHasEvaluationIfNeeded(&ride)
}

var errNoRides = fmt.Errorf("no rides")

func updateRideEvaluationInCache(rideID string, evaluation int, updatedAt time.Time) error {
	rideCacheMapRWMutex.Lock()
	defer rideCacheMapRWMutex.Unlock()

	ride, ok := rideCacheMap[rideID]
	if !ok {
		return errNoRides
	}

	ride.Evaluation = &evaluation
	ride.UpdatedAt = updatedAt
	// safe because Evaluation will be set only once
	updateRideCachePerChairAndHasEvaluationIfNeeded(ride)
	return nil
}

func updateRideChairIdInCache(rideID, chairID string, updatedAt time.Time) error {
	rideCacheMapRWMutex.Lock()
	defer rideCacheMapRWMutex.Unlock()

	ride, ok := rideCacheMap[rideID]
	if !ok {
		return errNoRides
	}

	ride.ChairID = sql.NullString{String: chairID, Valid: true}
	ride.UpdatedAt = updatedAt
	// safe because ChairID will be set only once
	updateRideCachePerChairAndHasEvaluationIfNeeded(ride)
	return nil
}

func getRideByIDFromCache(rideID string) (*Ride, bool) {
	rideCacheMapRWMutex.RLock()
	defer rideCacheMapRWMutex.RUnlock()

	ride, ok := rideCacheMap[rideID]
	return ride, ok
}

var userMapRWMutex = sync.RWMutex{}
var userMapCache map[string]*User = make(map[string]*User)
var accessTokenToUserCache map[string]*User = make(map[string]*User)

func loadUserMapCache() error {
	userMapRWMutex.Lock()
	defer userMapRWMutex.Unlock()

	users := []*User{}
	if err := db.Select(&users, "SELECT * FROM users"); err != nil {
		return err
	}

	userMapCache = make(map[string]*User)
	accessTokenToUserCache = make(map[string]*User)
	for _, user := range users {
		userMapCache[user.ID] = user
		accessTokenToUserCache[user.AccessToken] = user
	}
	return nil
}

func insertUserMapCache(user User) {
	userMapRWMutex.Lock()
	defer userMapRWMutex.Unlock()

	userMapCache[user.ID] = &user
	accessTokenToUserCache[user.AccessToken] = &user
}

func getUserByIDFromCache(userID string) (*User, bool) {
	userMapRWMutex.RLock()
	defer userMapRWMutex.RUnlock()

	user, ok := userMapCache[userID]
	return user, ok
}

func getUserByAccessToken(accessToken string) (*User, bool) {
	userMapRWMutex.RLock()
	defer userMapRWMutex.RUnlock()

	user, ok := accessTokenToUserCache[accessToken]
	return user, ok
}

var unsentRideStatusesToChairRWMutex = sync.RWMutex{}
var unsentRideStatusesToChairChan map[string](chan *chairGetNotificationResponseData) = make(map[string](chan *chairGetNotificationResponseData))
var sentLastRideStatusToChair map[string]*chairGetNotificationResponseData = make(map[string]*chairGetNotificationResponseData)

func loadUnsentRideStatusesToChair() error {
	unsentRideStatusesToChairRWMutex.Lock()
	defer unsentRideStatusesToChairRWMutex.Unlock()

	// all notifications should be sent before the server termination
	unsentRideStatusesToChairChan = make(map[string](chan *chairGetNotificationResponseData))
	sentLastRideStatusToChair = make(map[string]*chairGetNotificationResponseData)

	return nil
}

func appendChairGetNotificationResponseData(chairID string, data *chairGetNotificationResponseData) {
	unsentRideStatusesToChairRWMutex.Lock()
	defer unsentRideStatusesToChairRWMutex.Unlock()

	if _, ok := unsentRideStatusesToChairChan[chairID]; !ok {
		unsentRideStatusesToChairChan[chairID] = make(chan *chairGetNotificationResponseData, 10)
	}
	slog.Info("appendChairGetNotificationResponseData", "chairID", chairID, "data", data)
	unsentRideStatusesToChairChan[chairID] <- data
}

func getChairGetNotificationResponseDataChannel(chairID string) chan *chairGetNotificationResponseData {
	unsentRideStatusesToChairRWMutex.Lock()
	defer unsentRideStatusesToChairRWMutex.Unlock()
	if _, ok := unsentRideStatusesToChairChan[chairID]; !ok {
		unsentRideStatusesToChairChan[chairID] = make(chan *chairGetNotificationResponseData, 10)
	}
	return unsentRideStatusesToChairChan[chairID]
}

var ErrNoChairAssigned = fmt.Errorf("no chair assigned")

func buildChairGetNotificationResponseData(rideStatusId, rideId string, rideStatus string) (*Ride, *chairGetNotificationResponseData, error) {
	ride, found := getRideByIDFromCache(rideId)
	if !found {
		return nil, nil, errNoRides
	}

	if !ride.ChairID.Valid {
		// slog.Info("buildChairGetNotificationResponseData chair is not assigned yet", "ride", *ride, "rideStatus", rideStatus)
		return nil, nil, ErrNoChairAssigned
	}

	user, found := getUserByIDFromCache(ride.UserID)
	if !found {
		return nil, nil, errors.New("user not found")
	}

	b := &chairGetNotificationResponseData{
		RideStatusId: rideStatusId,
		RideID:       ride.ID,
		User: simpleUser{
			ID:   user.ID,
			Name: fmt.Sprintf("%s %s", user.Firstname, user.Lastname),
		},
		PickupCoordinate: Coordinate{
			Latitude:  ride.PickupLatitude,
			Longitude: ride.PickupLongitude,
		},
		DestinationCoordinate: Coordinate{
			Latitude:  ride.DestinationLatitude,
			Longitude: ride.DestinationLongitude,
		},
		Status: rideStatus,
	}

	slog.Info("buildChairGetNotificationResponseData - update status", "chair", ride.ChairID, "currentStatus", rideStatus, "b", b)

	return ride, b, nil
}

func buildAndAppendChairGetNotificationResponseData(rideStatusId, rideId string, rideStatus string) (*chairGetNotificationResponseData, error) {
	slog.Info("buildAndAppendChairGetNotificationResponseData", "rideStatusId", rideStatusId, "rideId", rideId, "rideStatus", rideStatus)
	ride, responseData, err := buildChairGetNotificationResponseData(rideStatusId, rideId, rideStatus)
	if err != nil {
		if errors.Is(err, ErrNoChairAssigned) {
			return nil, nil
		} else {
			return nil, err
		}
	}

	appendChairGetNotificationResponseData(ride.ChairID.String, responseData)
	return responseData, nil
}

func buildAppGetNotificationResponseData(rideStatusId, rideId string, rideStatus string) (*Ride, *appGetNotificationResponseData, error) {
	ride, found := getRideByIDFromCache(rideId)
	if !found {
		return nil, nil, errNoRides
	}

	_, found = getUserByIDFromCache(ride.UserID)
	if !found {
		return nil, nil, errors.New("user not found")
	}

	responseData := &appGetNotificationResponseData{
		RideStatusId: rideStatusId,
		RideID:       ride.ID,
		PickupCoordinate: Coordinate{
			Latitude:  ride.PickupLatitude,
			Longitude: ride.PickupLongitude,
		},
		DestinationCoordinate: Coordinate{
			Latitude:  ride.DestinationLatitude,
			Longitude: ride.DestinationLongitude,
		},
		Fare:      -1,
		Status:    rideStatus,
		CreatedAt: ride.CreatedAt.UnixMilli(),
		UpdateAt:  ride.UpdatedAt.UnixMilli(),
	}

	if ride.ChairID.Valid {
		chair, err := getChairByID(ride.ChairID.String)
		if err != nil {
			return nil, nil, err
		}

		stats, err := getChairStats(chair.ID)
		if err != nil {
			return nil, nil, err
		}

		responseData.Chair = &appGetNotificationResponseChair{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
			Stats: stats,
		}
	}

	slog.Info("buildChairGetNotificationResponseData - update status", "chair", ride.ChairID, "currentStatus", rideStatus, "b", responseData)
	return ride, responseData, nil
}

func buildAndAppendAppGetNotificationResponseData(rideStatusId, rideId string, rideStatus string) (*appGetNotificationResponseData, error) {
	slog.Info("buildAndAppendAppGetNotificationResponseData", "rideStatusId", rideStatusId, "rideId", rideId, "rideStatus", rideStatus)
	ride, responseData, err := buildAppGetNotificationResponseData(rideStatusId, rideId, rideStatus)
	if err != nil {
		if errors.Is(err, ErrNoChairAssigned) {
			return nil, nil
		} else {
			return nil, err
		}
	}

	appendAppGetNotificationResponseData(ride.UserID, responseData)
	return responseData, nil
}

func insertRideStatus(ctx context.Context, tx *sqlx.Tx, ride_id, status string) (*appGetNotificationResponseData, error) {
	id := ulid.Make().String()
	now := time.Now()
	_, err := tx.ExecContext(
		ctx,
		"INSERT INTO ride_statuses (id, ride_id, status, created_at) VALUES (?, ?, ?, ?)",
		id, ride_id, status, now)
	if err != nil {
		return nil, err
	}

	rideStatus := &RideStatus{
		ID:          id,
		RideID:      ride_id,
		Status:      status,
		CreatedAt:   now,
		AppSentAt:   nil,
		ChairSentAt: nil,
	}

	updateLatestRideStatusCacheMap(rideStatus)
	buildAndAppendChairGetNotificationResponseData(id, ride_id, status)
	response, _ := buildAndAppendAppGetNotificationResponseData(id, ride_id, status)

	return response, nil
}

func insertRideStatusWithoutTransaction(ctx context.Context, ride_id, status string) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := insertRideStatus(ctx, tx, ride_id, status); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func updateLatestRideStatusCacheMap(rideStatus *RideStatus) {
	latestRideStatusCacheMapRWMutex.Lock()
	defer latestRideStatusCacheMapRWMutex.Unlock()

	latestRideStatusCacheMap[rideStatus.RideID] = rideStatus
}

func getLatestRideStatusFromCache(ride_id string) (string, error) {
	latestRideStatusCacheMapRWMutex.RLock()
	defer latestRideStatusCacheMapRWMutex.RUnlock()

	rideStatus, ok := latestRideStatusCacheMap[ride_id]
	if !ok {
		return "", errors.New("ride status not found")
	}
	return rideStatus.Status, nil
}

func chairPostChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &chairPostChairsRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Model == "" || req.ChairRegisterToken == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name, model, chair_register_token) are empty"))
		return
	}

	owner := &Owner{}
	if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE chair_register_token = ?", req.ChairRegisterToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusUnauthorized, errors.New("invalid chair_register_token"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairID := ulid.Make().String()
	accessToken := secureRandomStr(32)

	now := time.Now()
	newChair := &Chair{
		ID:          chairID,
		OwnerID:     owner.ID,
		Name:        req.Name,
		Model:       req.Model,
		IsActive:    false,
		AccessToken: accessToken,
		CreatedAt:   now,
		UpdatedAt:   now,
		IsFree:      true,
	}

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO chairs (id, owner_id, name, model, is_active, access_token, created_at, updated_at, is_free) VALUES (?, ?, ?, ?, ?, ?, ?, ? ,?)",
		newChair.ID, newChair.OwnerID, newChair.Name, newChair.Model, newChair.IsActive, newChair.AccessToken, newChair.CreatedAt, newChair.UpdatedAt, newChair.IsFree,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	insertChairCacheMap(*newChair)

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "chair_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &chairPostChairsResponse{
		ID:      chairID,
		OwnerID: owner.ID,
	})
}

type postChairActivityRequest struct {
	IsActive bool `json:"is_active"`
}

func chairPostActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	req := &postChairActivityRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := db.ExecContext(ctx, "UPDATE chairs SET is_active = ? WHERE id = ?", req.IsActive, chair.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := updateIsActiveInCache(chair.ID, req.IsActive); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type chairPostCoordinateResponse struct {
	RecordedAt int64 `json:"recorded_at"`
}

func chairPostCoordinate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &Coordinate{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	chair := ctx.Value("chair").(*Chair)
	updatedAt := time.Now()

	// メモリ上を更新する
	chairLocationCacheMapRWMutex.Lock()
	cll, ok := chairLocationCacheMap[chair.ID]
	if !ok {
		cll = &ChairLocationLatest{
			ChairID:       chair.ID,
			Latitude:      req.Latitude,
			Longitude:     req.Longitude,
			UpdatedAt:     updatedAt,
			TotalDistance: 0,
			isDirty:       true,
		}
	} else {
		cll.TotalDistance += abs(cll.Latitude-req.Latitude) + abs(cll.Longitude-req.Longitude)
		cll.Latitude = req.Latitude
		cll.Longitude = req.Longitude
		cll.UpdatedAt = updatedAt
		cll.isDirty = true
	}
	chairLocationCacheMap[chair.ID] = cll
	chairLocationCacheMapRWMutex.Unlock()

	ride, _ := getLatestRideByChairId(chair.ID)

	if ride != nil {
		// status, err := getLatestRideStatu(ride.ID)
		status, err := getLatestRideStatusFromCache(ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if status != "COMPLETED" && status != "CANCELED" {
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				if err := insertRideStatusWithoutTransaction(ctx, ride.ID, "PICKUP"); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}

			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				if err := insertRideStatusWithoutTransaction(ctx, ride.ID, "ARRIVED"); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, &chairPostCoordinateResponse{
		RecordedAt: updatedAt.UnixMilli(),
	})
}

type simpleUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type chairGetNotificationResponse struct {
	Data         *chairGetNotificationResponseData `json:"data"`
	RetryAfterMs int                               `json:"retry_after_ms"`
}

type chairGetNotificationResponseData struct {
	RideStatusId          string     `json:"-"`
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

func chairGetNotificationSSE(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Type")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	c := getChairGetNotificationResponseDataChannel(chair.ID)

	for {
		select {
		case dataFromChannel := <-c:
			b, _ := json.Marshal(dataFromChannel)
			fmt.Fprintf(w, "data: %s\n", b)
			w.(http.Flusher).Flush()

			slog.Info("chairGetNotification - sent", "chair", chair.ID, "status", dataFromChannel.Status)

			rideStatusSentAtChan <- RideStatusSentAtRequest{
				RideStatusID: dataFromChannel.RideStatusId,
				RideID:       dataFromChannel.RideID,
				ChairID:      chair.ID,
				Status:       dataFromChannel.Status,
				SentType:     ChairNotification,
			}
		case <-r.Context().Done():
			return
		}
	}
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

type chairPostRideStatusUpdateRequest struct {
	rideID string
	status string
}

var chairPostRideStatusUpdateChan = make(chan chairPostRideStatusUpdateRequest, 1000)

func chairPostRideStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	chair := ctx.Value("chair").(*Chair)

	req := &postChairRidesRideIDStatusRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, "SELECT * FROM rides WHERE id = ? FOR UPDATE", rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if ride.ChairID.String != chair.ID {
		writeError(w, http.StatusBadRequest, errors.New("not assigned to this ride"))
		return
	}

	switch req.Status {
	// Acknowledge the ride
	case "ENROUTE":
		chairPostRideStatusUpdateChan <- chairPostRideStatusUpdateRequest{
			rideID: rideID,
			status: "ENROUTE",
		}
		// if err := insertRideStatus(ctx, tx, ride.ID, "ENROUTE"); err != nil {
		// 	writeError(w, http.StatusInternalServerError, err)
		// 	return
		// }
	// After Picking up user
	case "CARRYING":
		status, err := getLatestRideStatusFromCache(ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "PICKUP" {
			writeError(w, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		chairPostRideStatusUpdateChan <- chairPostRideStatusUpdateRequest{
			rideID: rideID,
			status: "CARRYING",
		}
		// if err := insertRideStatus(ctx, tx, ride.ID, "CARRYING"); err != nil {
		// 	writeError(w, http.StatusInternalServerError, err)
		// 	return
		// }
	default:
		writeError(w, http.StatusBadRequest, errors.New("invalid status"))
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func launchChairPostRideStatusSyncer() {
	go func() {
		for req := range chairPostRideStatusUpdateChan {
			insertRideStatusWithoutTransaction(context.Background(), req.rideID, req.status)
		}
	}()
}
