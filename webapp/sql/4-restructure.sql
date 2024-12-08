DROP TABLE IF EXISTS chair_locations_latest;
CREATE TABLE chair_locations_latest
(
  chair_id   VARCHAR(26) NOT NULL COMMENT '椅子ID',
  latitude   INTEGER     NOT NULL COMMENT '経度',
  longitude  INTEGER     NOT NULL COMMENT '緯度',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  total_distance INTEGER NOT NULL DEFAULT 0 COMMENT '合計移動距離',
  PRIMARY KEY (chair_id)
)
SELECT chair_id, latitude, longitude, created_at
  FROM chair_locations
  WHERE created_at = (SELECT MAX(created_at) FROM chair_locations WHERE chair_id = chair_locations.chair_id);
FROM chair_locations;

// 以下のSQLを実行して、合計移動距離を計算し、chair_locations_latestテーブルに挿入してください
SELECT id,
       IFNULL(total_distance, 0) AS total_distance,
       total_distance_updated_at
FROM chairs
       LEFT JOIN (SELECT chair_id,
                          SUM(IFNULL(distance, 0)) AS total_distance,
                          MAX(created_at)          AS total_distance_updated_at
                   FROM (SELECT chair_id,
                                created_at,
                                ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
                                ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
                         FROM chair_locations) tmp
                   GROUP BY chair_id) distance_table ON distance_table.chair_id = chairs.id

