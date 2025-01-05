package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB

var paymentGatewayURL string

func loadPaymentGatewayURL(ctx context.Context) error {
	return db.GetContext(ctx, &paymentGatewayURL, "SELECT value FROM settings WHERE name = 'payment_gateway_url'")
}

func main() {
	mux := setup()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError.Level(),
	}))
	slog.SetDefault(logger)
	slog.Info("Listening on :8080")
	http.ListenAndServe(":8080", mux)
}

func setup() http.Handler {
	host := os.Getenv("ISUCON_DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("ISUCON_DB_PORT")
	if port == "" {
		port = "3306"
	}
	_, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("failed to convert DB port number from ISUCON_DB_PORT environment variable into int: %v", err))
	}
	user := os.Getenv("ISUCON_DB_USER")
	if user == "" {
		user = "isucon"
	}
	password := os.Getenv("ISUCON_DB_PASSWORD")
	if password == "" {
		password = "isucon"
	}
	dbname := os.Getenv("ISUCON_DB_NAME")
	if dbname == "" {
		dbname = "isuride"
	}

	dbConfig := mysql.NewConfig()
	dbConfig.User = user
	dbConfig.Passwd = password
	dbConfig.Addr = net.JoinHostPort(host, port)
	dbConfig.Net = "tcp"
	dbConfig.DBName = dbname
	dbConfig.ParseTime = true
	dbConfig.InterpolateParams = true

	_db, err := sqlx.Connect("mysql", dbConfig.FormatDSN())
	if err != nil {
		panic(err)
	}
	db = _db

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)

	useMatching := false
	if os.Getenv("ISUCON_MATCHING") == "true" {
		useMatching = true
	}

	// 定期的にChairLocationLatestを保存する処理
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for range ticker.C {
			ctx := context.Background()
			func() {
				tx, err := db.Begin()
				if err != nil {
					slog.Error("failed to begin tx", "error", err)
					return
				}
				defer tx.Commit()

				chairLocationCacheMapRWMutex.Lock()
				defer chairLocationCacheMapRWMutex.Unlock()

				for _, cll := range chairLocationCacheMap {
					if cll.isDirty { // now < cll.UpdatedAt
						// 更新されているのでDBに保存する
						if _, err := tx.ExecContext(
							ctx,
							`INSERT INTO chair_locations_latest (chair_id, latitude, longitude, updated_at, total_distance) VALUES (?, ?, ?, ?, ?)
							ON DUPLICATE KEY UPDATE 
								latitude = ?, longitude = ?, updated_at = ?, total_distance = ?`,
							cll.ChairID, cll.Latitude, cll.Longitude, cll.UpdatedAt, cll.TotalDistance, cll.Latitude, cll.Longitude, cll.UpdatedAt, cll.TotalDistance,
						); err != nil {
							slog.Error("failed to insert chair location", "error", err)
						}
						cll.isDirty = false
					}
				}

			}()
		}
	}()

	// 定期的にマッチングを行う処理
	if useMatching {
		slog.Info("use matching")
		go func() {
			for {
				runMatching()
				<-time.After(50 * time.Millisecond)
			}
		}()
	} else {
		slog.Warn("not use matching")
	}

	if err := loadChairLocationCache(context.Background()); err != nil {
		slog.Error("failed to load chair location cache", "error", err)
	}

	if err := loadLatestRideStatusCacheMap(); err != nil {
		slog.Error("failed to load latest ride status cache", "error", err)
	}
	if err := loadChairCacheMap(); err != nil {
		slog.Error("failed to load chair cache", "error", err)
	}
	if err := loadLatestRideToChairAssignments(); err != nil {
		slog.Error("failed to load latest ride to chair assignments", "error", err)
	}
	if err := loadUnsentRideStatusesToChair(); err != nil {
		slog.Error("failed to load unsent ride statuses to chair", "error", err)
	}
	if err := loadUnsentRideStatusesToApp(); err != nil {
		slog.Error("failed to load unsent ride statuses to app", "error", err)
	}

	if err := loadPaymentGatewayURL(context.Background()); err != nil {
		slog.Error("failed to load payment gateway url", "error", err)
	}

	if err := loadRideIdToCouponMap(); err != nil {
		slog.Error("failed to load ride id to coupon map", "error", err)
	}

	launchRideStatusSentAtSyncer()
	launchChairPostRideStatusSyncer()

	mux := chi.NewRouter()
	mux.Use(middleware.Recoverer)
	mux.HandleFunc("POST /api/initialize", postInitialize)

	// app handlers
	{
		mux.HandleFunc("POST /api/app/users", appPostUsers)

		authedMux := mux.With(appAuthMiddleware)
		authedMux.HandleFunc("POST /api/app/payment-methods", appPostPaymentMethods)
		authedMux.HandleFunc("GET /api/app/rides", appGetRides)
		authedMux.HandleFunc("POST /api/app/rides", appPostRides)
		authedMux.HandleFunc("POST /api/app/rides/estimated-fare", appPostRidesEstimatedFare)
		authedMux.HandleFunc("POST /api/app/rides/{ride_id}/evaluation", appPostRideEvaluatation)
		authedMux.HandleFunc("GET /api/app/notification", appGetNotificationSSE)
		authedMux.HandleFunc("GET /api/app/nearby-chairs", appGetNearbyChairs)
	}

	// owner handlers
	{
		mux.HandleFunc("POST /api/owner/owners", ownerPostOwners)

		authedMux := mux.With(ownerAuthMiddleware)
		authedMux.HandleFunc("GET /api/owner/sales", ownerGetSales)
		authedMux.HandleFunc("GET /api/owner/chairs", ownerGetChairs)
	}

	// chair handlers
	{
		mux.HandleFunc("POST /api/chair/chairs", chairPostChairs)

		authedMux := mux.With(chairAuthMiddleware)
		authedMux.HandleFunc("POST /api/chair/activity", chairPostActivity)
		authedMux.HandleFunc("POST /api/chair/coordinate", chairPostCoordinate)
		authedMux.HandleFunc("GET /api/chair/notification", chairGetNotificationSSE)
		authedMux.HandleFunc("POST /api/chair/rides/{ride_id}/status", chairPostRideStatus)
	}

	// internal handlers
	// {
	// 	mux.HandleFunc("GET /api/internal/matching", internalGetMatching)
	// }

	return mux
}

type postInitializeRequest struct {
	PaymentServer string `json:"payment_server"`
}

type postInitializeResponse struct {
	Language string `json:"language"`
}

func loadChairLocationCache(ctx context.Context) error {
	// DBからキャッシュとしてメモリにロード
	chairLocationCacheMapRWMutex.Lock()
	defer chairLocationCacheMapRWMutex.Unlock()

	chairLocationCacheMap = map[string]*ChairLocationLatest{}
	locations := []ChairLocationLatest{}
	if err := db.SelectContext(ctx, &locations, `SELECT * FROM chair_locations_latest`); err != nil {
		return err
	}

	for _, location := range locations {
		chairLocationCacheMap[location.ChairID] = &location
	}
	return nil
}

func postInitialize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &postInitializeRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if out, err := exec.Command("../sql/init.sh").CombinedOutput(); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize: %s: %w", string(out), err))
		return
	}

	if _, err := db.ExecContext(ctx, "UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'", req.PaymentServer); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := loadChairLocationCache(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := loadLatestRideStatusCacheMap(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := loadChairCacheMap(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := loadLatestRideToChairAssignments(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := loadUnsentRideStatusesToChair(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := loadUnsentRideStatusesToApp(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := loadPaymentGatewayURL(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := loadRideIdToCouponMap(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, postInitializeResponse{Language: "go"})
}

type Coordinate struct {
	Latitude  int `json:"latitude"`
	Longitude int `json:"longitude"`
}

func bindJSON(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	buf, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(statusCode)
	w.Write(buf)
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(statusCode)
	buf, marshalError := json.Marshal(map[string]string{"message": err.Error()})
	if marshalError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"marshaling error failed"}`))
		return
	}
	w.Write(buf)

	slog.Error("error response wrote", err)
}

func secureRandomStr(b int) string {
	k := make([]byte, b)
	if _, err := crand.Read(k); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", k)
}
