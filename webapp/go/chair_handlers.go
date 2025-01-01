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

	for _, rideStatus := range rideStatuses {
		if _, ok := latestRideStatusCacheMap[rideStatus.RideID]; !ok {
			latestRideStatusCacheMap[rideStatus.RideID] = rideStatus
		}
	}
	return nil
}

func insertRideStatus(ctx context.Context, tx *sqlx.Tx, ride_id, status string) error {
	id := ulid.Make().String()
	now := time.Now()
	_, err := tx.ExecContext(
		ctx,
		"INSERT INTO ride_statuses (id, ride_id, status, created_at) VALUES (?, ?, ?, ?)",
		id, ride_id, status, now)
	if err != nil {
		return err
	}

	rideStatus := &RideStatus{
		ID:          id,
		RideID:      ride_id,
		Status:      status,
		CreatedAt:   now,
		AppSentAt:   nil,
		ChairSentAt: nil,
	}
	onInsertRideStatus(rideStatus)

	return nil
}

func onInsertRideStatus(rideStatus *RideStatus) {
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

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES (?, ?, ?, ?, ?, ?)",
		chairID, owner.ID, req.Name, req.Model, false, accessToken,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

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

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

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

	rideIdFromCache, _ := getLatestRideIdByChairId(chair.ID)

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	} else {
		// status, err := getLatestRideStatu(ride.ID)
		status, err := getLatestRideStatusFromCache(ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if ride.ID != rideIdFromCache {
			writeError(w, http.StatusInternalServerError, errors.New("ride_id mismatch"))
			return
		}

		if status != "COMPLETED" && status != "CANCELED" {
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				if err := insertRideStatus(ctx, tx, ride.ID, "PICKUP"); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}

			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				if err := insertRideStatus(ctx, tx, ride.ID, "ARRIVED"); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
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
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Type")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for {
		d, err := getChairNotification(ctx, chair)

		b, _ := json.Marshal(d)
		fmt.Fprintf(w, "data: %s\n", b)
		w.(http.Flusher).Flush()

		if errors.Is(err, ErrNoChairs) {
			// retry
			time.Sleep(time.Duration(chairRetryAfterMs) * time.Millisecond)
			continue
		} else if err != nil {
			slog.Error("chairGetNotification", "error", err)
			return
		}

		select {
		case <-r.Context().Done():
			return
		default:
			time.Sleep(time.Duration(appNotifyMs) * time.Millisecond)
		}
	}
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

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
		if err := insertRideStatus(ctx, tx, ride.ID, "ENROUTE"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	// After Picking up user
	case "CARRYING":
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "PICKUP" {
			writeError(w, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		if err := insertRideStatus(ctx, tx, ride.ID, "CARRYING"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	default:
		writeError(w, http.StatusBadRequest, errors.New("invalid status"))
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
