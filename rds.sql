CREATE database analytics;

DROP TABLE analytics.results;

TRUNCATE analytics.results; 

CREATE TABLE analytics.results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    emotion VARCHAR(255),
    age_range_low INT,
    age_range_high INT,
    gender VARCHAR(255),
    image_name VARCHAR(1000),
    image_url VARCHAR(1000),
    face_id VARCHAR(1000),
    similar_face_ids JSON DEFAULT NULL,
    response TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM analytics.results;

DELETE  FROM analytics.results
WHERE id > 3;