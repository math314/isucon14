package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
)

type appPostUsersRequest struct {
	Username       string  `json:"username"`
	FirstName      string  `json:"firstname"`
	LastName       string  `json:"lastname"`
	DateOfBirth    string  `json:"date_of_birth"`
	InvitationCode *string `json:"invitation_code"`
}

type appPostUsersResponse struct {
	ID             string `json:"id"`
	InvitationCode string `json:"invitation_code"`
}

func appPostUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostUsersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Username == "" || req.FirstName == "" || req.LastName == "" || req.DateOfBirth == "" {
		writeError(w, http.StatusBadRequest, errors.New("required fields(username, firstname, lastname, date_of_birth) are empty"))
		return
	}

	userID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	invitationCode := secureRandomStr(15)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	now := time.Now().Truncate(time.Microsecond)
	newUser := User{
		ID:             userID,
		Username:       req.Username,
		Firstname:      req.FirstName,
		Lastname:       req.LastName,
		DateOfBirth:    req.DateOfBirth,
		AccessToken:    accessToken,
		InvitationCode: invitationCode,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		newUser.ID, newUser.Username, newUser.Firstname, newUser.Lastname, newUser.DateOfBirth, newUser.AccessToken, newUser.InvitationCode, newUser.CreatedAt, newUser.UpdatedAt,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		slog.Error("1", "error", err)
		return
	}
	insertUserMapCache(newUser)

	// 初回登録キャンペーンのクーポンを付与
	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
		userID, "CP_NEW2024", 3000,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		slog.Error("2", "error", err)
		return
	}

	// 招待コードを使った登録
	if req.InvitationCode != nil && *req.InvitationCode != "" {
		// ユーザーチェック
		var inviter User
		err = tx.GetContext(ctx, &inviter, "SELECT * FROM users WHERE invitation_code = ?", *req.InvitationCode)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
				slog.Error("3", "error", err)
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			slog.Error("4", "error", err)
			return
		}

		// 招待する側の招待数をチェック
		var coupons []Coupon
		err = tx.SelectContext(ctx, &coupons, "SELECT * FROM coupons WHERE code = ? LIMIT 4 FOR UPDATE", "INV_"+*req.InvitationCode)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			slog.Error("5", "error", err)
			return
		}
		if len(coupons) >= 3 {
			writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
			slog.Error("6", "error", err)
			return
		}

		// 招待クーポン付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
			userID, "INV_"+*req.InvitationCode, 1500,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			slog.Error("7", "error", err)
			return
		}
		// 招待した人にもRewardを付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, CONCAT(?, '_', FLOOR(UNIX_TIMESTAMP(NOW(3))*1000)), ?)",
			inviter.ID, "RWD_"+*req.InvitationCode, 1000,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			slog.Error("8", "error", err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		slog.Error("9", "error", err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "app_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &appPostUsersResponse{
		ID:             userID,
		InvitationCode: invitationCode,
	})
}

type appPostPaymentMethodsRequest struct {
	Token string `json:"token"`
}

func appPostPaymentMethods(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostPaymentMethodsRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, errors.New("token is required but was empty"))
		return
	}

	user := ctx.Value("user").(*User)

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO payment_tokens (user_id, token) VALUES (?, ?)`,
		user.ID,
		req.Token,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type getAppRidesResponse struct {
	Rides []getAppRidesResponseItem `json:"rides"`
}

type getAppRidesResponseItem struct {
	ID                    string                       `json:"id"`
	PickupCoordinate      Coordinate                   `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                   `json:"destination_coordinate"`
	Chair                 getAppRidesResponseItemChair `json:"chair"`
	Fare                  int                          `json:"fare"`
	Evaluation            int                          `json:"evaluation"`
	RequestedAt           int64                        `json:"requested_at"`
	CompletedAt           int64                        `json:"completed_at"`
}

type getAppRidesResponseItemChair struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	Model string `json:"model"`
}

func appGetRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	rides := []Ride{}
	if err := tx.SelectContext(
		ctx,
		&rides,
		`SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC`,
		user.ID,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	items := []getAppRidesResponseItem{}
	for _, ride := range rides {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "COMPLETED" {
			continue
		}

		fare, err := calculateDiscountedFare(ctx, tx, user.ID, ride.ID, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		item := getAppRidesResponseItem{
			ID:                    ride.ID,
			PickupCoordinate:      Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude},
			DestinationCoordinate: Coordinate{Latitude: ride.DestinationLatitude, Longitude: ride.DestinationLongitude},
			Fare:                  fare,
			Evaluation:            *ride.Evaluation,
			RequestedAt:           ride.CreatedAt.UnixMilli(),
			CompletedAt:           ride.UpdatedAt.UnixMilli(),
		}

		item.Chair = getAppRidesResponseItemChair{}

		chair := &Chair{}
		if err := tx.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, ride.ChairID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		item.Chair.ID = chair.ID
		item.Chair.Name = chair.Name
		item.Chair.Model = chair.Model

		owner := &Owner{}
		if err := tx.GetContext(ctx, owner, `SELECT * FROM owners WHERE id = ?`, chair.OwnerID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		item.Chair.Owner = owner.Name

		items = append(items, item)
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &getAppRidesResponse{
		Rides: items,
	})
}

type appPostRidesRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesResponse struct {
	RideID string `json:"ride_id"`
	Fare   int    `json:"fare"`
}

type executableGet interface {
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

func getLatestRideStatus(ctx context.Context, tx executableGet, rideID string) (string, error) {
	status := ""
	if err := tx.GetContext(ctx, &status, `SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1`, rideID); err != nil {
		return "", err
	}
	return status, nil
}

var rideIdToCouponMapRWMutex = sync.RWMutex{}
var rideIdToCouponMap map[string]*Coupon = make(map[string]*Coupon)

func loadRideIdToCouponMap() error {
	rideIdToCouponMapRWMutex.Lock()
	defer rideIdToCouponMapRWMutex.Unlock()

	rideIdToCouponMap = make(map[string]*Coupon)
	coupons := []Coupon{}
	if err := db.Select(&coupons, "SELECT * FROM coupons WHERE used_by IS NOT NULL"); err != nil {
		return err
	}

	for _, coupon := range coupons {
		if coupon.UsedBy != nil {
			rideIdToCouponMap[*coupon.UsedBy] = &coupon
		}
	}
	return nil
}

func updateRideIdToCouponMap(rideID string, coupon *Coupon) {
	rideIdToCouponMapRWMutex.Lock()
	defer rideIdToCouponMapRWMutex.Unlock()

	rideIdToCouponMap[rideID] = coupon
}

func getRideIdToCouponMap(rideID string) (*Coupon, bool) {
	rideIdToCouponMapRWMutex.RLock()
	defer rideIdToCouponMapRWMutex.RUnlock()

	coupon, ok := rideIdToCouponMap[rideID]
	return coupon, ok
}

func appPostRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)
	rideID := ulid.Make().String()

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// 既に進行中のライドがある場合はエラー
	continuingRideCount := 0
	if err := tx.GetContext(ctx, &continuingRideCount, `SELECT COUNT(*) FROM rides WHERE user_id = ? AND (SELECT status FROM ride_statuses WHERE ride_id = rides.id ORDER BY created_at DESC LIMIT 1) != 'COMPLETED'`, user.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if continuingRideCount > 0 {
		writeError(w, http.StatusConflict, errors.New("ride already exists"))
		return
	}

	now := time.Now().Truncate(time.Microsecond)
	newRide := Ride{
		ID:                   rideID,
		UserID:               user.ID,
		ChairID:              sql.NullString{},
		PickupLatitude:       req.PickupCoordinate.Latitude,
		PickupLongitude:      req.PickupCoordinate.Longitude,
		DestinationLatitude:  req.DestinationCoordinate.Latitude,
		DestinationLongitude: req.DestinationCoordinate.Longitude,
		Evaluation:           nil,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO rides (id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		newRide.ID, newRide.UserID, newRide.PickupLatitude, newRide.PickupLongitude, newRide.DestinationLatitude, newRide.DestinationLongitude, newRide.CreatedAt, newRide.UpdatedAt,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	insertRideCacheMap(newRide)

	if _, err := insertRideStatus(ctx, tx, rideID, "MATCHING"); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	var rideCount int
	if err := tx.GetContext(ctx, &rideCount, `SELECT COUNT(*) FROM rides WHERE user_id = ? `, user.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	var coupon Coupon
	if rideCount == 1 {
		// 初回利用で、初回利用クーポンがあれば必ず使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}

			// 無ければ他のクーポンを付与された順番に使う
			if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			} else {
				if _, err := tx.ExecContext(
					ctx,
					"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
					rideID, user.ID, coupon.Code,
				); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = 'CP_NEW2024'",
				rideID, user.ID,
			); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	} else {
		// 他のクーポンを付与された順番に使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
				rideID, user.ID, coupon.Code,
			); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	}

	usedCoupon, err := func() (*Coupon, error) {
		coupon := &Coupon{}
		if err := tx.GetContext(ctx, coupon, "SELECT * FROM coupons WHERE used_by = ?", rideID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			} else {
				writeError(w, http.StatusInternalServerError, err)
				return nil, err
			}
		}
		return coupon, nil
	}()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if usedCoupon != nil {
		updateRideIdToCouponMap(rideID, usedCoupon)
	}

	_, found := getRideByIDFromCache(rideID)
	if !found {
		writeError(w, http.StatusInternalServerError, errNoRides)
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, user.ID, rideID, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusAccepted, &appPostRidesResponse{
		RideID: rideID,
		Fare:   fare,
	})
}

type appPostRidesEstimatedFareRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesEstimatedFareResponse struct {
	Fare     int `json:"fare"`
	Discount int `json:"discount"`
}

func appPostRidesEstimatedFare(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesEstimatedFareRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	discounted, err := calculateDiscountedFare(ctx, tx, user.ID, "", req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRidesEstimatedFareResponse{
		Fare:     discounted,
		Discount: calculateFare(req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude) - discounted,
	})
}

// マンハッタン距離を求める
func calculateDistance(aLatitude, aLongitude, bLatitude, bLongitude int) int {
	return abs(aLatitude-bLatitude) + abs(aLongitude-bLongitude)
}
func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

type appPostRideEvaluationRequest struct {
	Evaluation int `json:"evaluation"`
}

type appPostRideEvaluationResponse struct {
	CompletedAt int64 `json:"completed_at"`
}

func appPostRideEvaluatation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	req := &appPostRideEvaluationRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Evaluation < 1 || req.Evaluation > 5 {
		writeError(w, http.StatusBadRequest, errors.New("evaluation must be between 1 and 5"))
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride, found := getRideByIDFromCache(rideID)
	if !found {
		writeError(w, http.StatusNotFound, errors.New("ride not found"))
		return
	}

	status, err := getLatestRideStatusFromCache(ride.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if status != "ARRIVED" {
		writeError(w, http.StatusBadRequest, errors.New("not arrived yet"))
		return
	}

	updatedAt := time.Now().Truncate(time.Microsecond)
	result, err := tx.ExecContext(
		ctx,
		`UPDATE rides SET evaluation = ?, updated_at = ? WHERE id = ?`,
		req.Evaluation, updatedAt, rideID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if count, err := result.RowsAffected(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	} else if count == 0 {
		writeError(w, http.StatusNotFound, errors.New("ride not found"))
		return
	}
	if err := updateRideEvaluationInCache(rideID, req.Evaluation, updatedAt); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	paymentToken := &PaymentToken{}
	if err := tx.GetContext(ctx, paymentToken, `SELECT * FROM payment_tokens WHERE user_id = ?`, ride.UserID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusBadRequest, errors.New("payment token not registered"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, ride.UserID, ride.ID, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	paymentGatewayRequest := &paymentGatewayPostPaymentRequest{
		Amount: fare,
	}

	if err := requestPaymentGatewayPostPayment(ctx, paymentGatewayURL, paymentToken.Token, paymentGatewayRequest); err != nil {
		if errors.Is(err, erroredUpstream) {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairGetNotificationResponseData, err := insertRideStatus(ctx, tx, rideID, "COMPLETED")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRideEvaluationResponse{
		CompletedAt: updatedAt.UnixMilli(),
	})

	rideStatusSentAtChan <- RideStatusSentAtRequest{
		RideStatusID: chairGetNotificationResponseData.RideStatusId,
		RideID:       chairGetNotificationResponseData.RideID,
		ChairID:      chairGetNotificationResponseData.Chair.ID,
		Status:       chairGetNotificationResponseData.Status,
		SentType:     EvaluationResultFlushed,
	}
}

type appGetNotificationResponseData struct {
	RideStatusId          string                           `json:"-"`
	RideID                string                           `json:"ride_id"`
	PickupCoordinate      Coordinate                       `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                       `json:"destination_coordinate"`
	Fare                  int                              `json:"fare"`
	Status                string                           `json:"status"`
	Chair                 *appGetNotificationResponseChair `json:"chair,omitempty"`
	CreatedAt             int64                            `json:"created_at"`
	UpdateAt              int64                            `json:"updated_at"`
}

type appGetNotificationResponseChair struct {
	ID    string                               `json:"id"`
	Name  string                               `json:"name"`
	Model string                               `json:"model"`
	Stats appGetNotificationResponseChairStats `json:"stats"`
}

type appGetNotificationResponseChairStats struct {
	TotalRidesCount    int     `json:"total_rides_count"`
	TotalEvaluationAvg float64 `json:"total_evaluation_avg"`
}

var unsentRideStatusesToAppRWMutex = sync.RWMutex{}
var unsentRideStatusesToAppChan map[string](chan *appGetNotificationResponseData) = make(map[string](chan *appGetNotificationResponseData))

func loadUnsentRideStatusesToApp() error {
	unsentRideStatusesToAppRWMutex.Lock()
	defer unsentRideStatusesToAppRWMutex.Unlock()

	// all notifications should be sent before the server termination
	unsentRideStatusesToAppChan = make(map[string](chan *appGetNotificationResponseData))
	return nil
}

func appendAppGetNotificationResponseData(userID string, data *appGetNotificationResponseData) {
	unsentRideStatusesToAppRWMutex.Lock()
	defer unsentRideStatusesToAppRWMutex.Unlock()
	if _, ok := unsentRideStatusesToAppChan[userID]; !ok {
		unsentRideStatusesToAppChan[userID] = make(chan *appGetNotificationResponseData, 10)
	}
	unsentRideStatusesToAppChan[userID] <- data
}

func getAppGetNotificationResponseDataChannel(userID string) chan *appGetNotificationResponseData {
	unsentRideStatusesToAppRWMutex.Lock()
	defer unsentRideStatusesToAppRWMutex.Unlock()
	if _, ok := unsentRideStatusesToAppChan[userID]; !ok {
		unsentRideStatusesToAppChan[userID] = make(chan *appGetNotificationResponseData, 10)
	}
	return unsentRideStatusesToAppChan[userID]
}

func appGetNotificationSSE(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Type")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	c := getAppGetNotificationResponseDataChannel(user.ID)

	for {
		select {
		case dataFromChannel := <-c:
			tx, err := db.Beginx()
			if err != nil {
				slog.Error("appGetNotificationSSE - failed to begin transaction", "error", err)
				return
			}
			dataFromChannel.Fare, err = calculateDiscountedFare(ctx, tx, user.ID, dataFromChannel.RideID, dataFromChannel.PickupCoordinate.Latitude, dataFromChannel.PickupCoordinate.Longitude, dataFromChannel.DestinationCoordinate.Latitude, dataFromChannel.DestinationCoordinate.Longitude)
			tx.Rollback()
			if err != nil {
				slog.Error("appGetNotificationSSE - failed to calculate fare", "error", err)
				return
			}

			b, _ := json.Marshal(dataFromChannel)
			fmt.Fprintf(w, "data: %s\n", b)
			w.(http.Flusher).Flush()

			chairID := ""
			if dataFromChannel.Chair != nil {
				chairID = dataFromChannel.Chair.ID
			}

			slog.Info("appGetNotificationSSE - sent", "chair", chairID, "status", dataFromChannel.Status)

			rideStatusSentAtChan <- RideStatusSentAtRequest{
				RideStatusID: dataFromChannel.RideStatusId,
				RideID:       dataFromChannel.RideID,
				ChairID:      chairID,
				Status:       dataFromChannel.Status,
				SentType:     AppNotification,
			}

		case <-r.Context().Done():
			return
		}
	}
}

func getChairStats(chairID string) (appGetNotificationResponseChairStats, error) {
	stats := appGetNotificationResponseChairStats{}

	totalCountAndTotalEvaluation := getRideCachePerChairAndHasEvaluation(chairID)

	stats.TotalRidesCount = totalCountAndTotalEvaluation.TotalCount
	if totalCountAndTotalEvaluation.TotalCount > 0 {
		stats.TotalEvaluationAvg = float64(totalCountAndTotalEvaluation.TotalEvaluationSum) / float64(totalCountAndTotalEvaluation.TotalCount)
	}

	return stats, nil
}

type appGetNearbyChairsResponse struct {
	Chairs      []appGetNearbyChairsResponseChair `json:"chairs"`
	RetrievedAt int64                             `json:"retrieved_at"`
}

type appGetNearbyChairsResponseChair struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Model             string     `json:"model"`
	CurrentCoordinate Coordinate `json:"current_coordinate"`
}

type LatLon struct {
	Lat int `db:"latitude"`
	Lon int `db:"longitude"`
}

func appGetNearbyChairs(w http.ResponseWriter, r *http.Request) {
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	distanceStr := r.URL.Query().Get("distance")
	if latStr == "" || lonStr == "" {
		writeError(w, http.StatusBadRequest, errors.New("latitude or longitude is empty"))
		return
	}

	lat, err := strconv.Atoi(latStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("latitude is invalid"))
		return
	}

	lon, err := strconv.Atoi(lonStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("longitude is invalid"))
		return
	}

	distance := 50
	if distanceStr != "" {
		distance, err = strconv.Atoi(distanceStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("distance is invalid"))
			return
		}
	}

	coordinate := Coordinate{Latitude: lat, Longitude: lon}

	// slog.Info("appGetNearbyChairs - start", "coordinate", coordinate, "distance", distance)

	retrievedAt := time.Now().Add(-1 * time.Millisecond)

	chairCacheMapRWMutex.RLock()
	chairLocationCacheMapRWMutex.RLock()
	chairIdToLatestRideIdMutex.RLock()
	latestRideStatusCacheMapRWMutex.RLock()

	nearbyChairs := []appGetNearbyChairsResponseChair{}
	for _, chair := range chairCacheMap {
		// slog.Info("  appGetNearbyChairs chair loop", "coordinate", coordinate, "chair", chair)
		if !chair.IsActive || !chair.IsFree {
			// slog.Info("  appGetNearbyChairs chair loop - not active or not free", "coordinate", coordinate, "chair", chair)
			continue
		}
		loc, ok := chairLocationCacheMap[chair.ID]
		if !ok {
			// slog.Info("  appGetNearbyChairs chair loop - no location found", "coordinate", coordinate, "chair", chair)
			continue
		}
		currentDist := calculateDistance(coordinate.Latitude, coordinate.Longitude, loc.Latitude, loc.Longitude)
		if currentDist > distance {
			// slog.Info("  appGetNearbyChairs chair loop - too far", "chair", chair, "coordinate", coordinate, "loc", loc, "currentDist", currentDist, "distance", distance)
			continue
		}
		ride, rideFound := chairIdToLatestRideId[chair.ID]
		if rideFound {
			rideStatus, rideStatusFound := latestRideStatusCacheMap[ride.ID]
			if rideStatusFound && rideStatus.Status != "COMPLETED" {
				// slog.Info("  appGetNearbyChairs chair loop - ride status found but not completed", "coordinate", coordinate, "chair", chair, "ride", ride, "rideStatus", rideStatus)
				continue
			}
		}

		nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChair{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
			CurrentCoordinate: Coordinate{
				Latitude:  loc.Latitude,
				Longitude: loc.Longitude,
			},
		})
	}

	latestRideStatusCacheMapRWMutex.RUnlock()
	chairIdToLatestRideIdMutex.RUnlock()
	chairLocationCacheMapRWMutex.RUnlock()
	chairCacheMapRWMutex.RUnlock()

	slog.Info("appGetNearbyChairs - result", "coordinate", coordinate, "distance", distance, "nearbyChairs", nearbyChairs)

	writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{
		Chairs:      nearbyChairs,
		RetrievedAt: retrievedAt.UnixMilli(),
	})
}

func calculateFare(pickupLatitude, pickupLongitude, destLatitude, destLongitude int) int {
	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	return initialFare + meteredFare
}

func calculateDiscountedFare(ctx context.Context, tx *sqlx.Tx, userID string, rideId string, pickupLatitude, pickupLongitude, destLatitude, destLongitude int) (int, error) {
	var coupon Coupon
	discount := 0
	if rideId != "" {
		// destLatitude = ride.DestinationLatitude
		// destLongitude = ride.DestinationLongitude
		// pickupLatitude = ride.PickupLatitude
		// pickupLongitude = ride.PickupLongitude

		// すでにクーポンが紐づいているならそれの割引額を参照
		coupon, couponFound := getRideIdToCouponMap(rideId)
		if couponFound {
			discount = coupon.Discount
			// slog.Info("calculateDiscountedFare - coupon is used when ride is provided", "coupon", coupon, "ride", ride)
		}
	} else {
		// 初回利用クーポンを最優先で使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL", userID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}

			// 無いなら他のクーポンを付与された順番に使う
			if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1", userID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
			} else {
				discount = coupon.Discount
			}
		} else {
			discount = coupon.Discount
		}
	}

	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	discountedMeteredFare := max(meteredFare-discount, 0)

	return initialFare + discountedMeteredFare, nil
}
