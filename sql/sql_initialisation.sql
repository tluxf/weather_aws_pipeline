DROP TABLE IF EXISTS weather_current;
CREATE TABLE weather_current (
    id SERIAL PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    pressure DECIMAL(6,2),
    wind_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS weather_predict_6h;
CREATE TABLE weather_predict_6h (
    id SERIAL PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    pressure DECIMAL(6,2),
    wind_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS weather_predict_1d;
CREATE TABLE weather_predict_1d (
    id SERIAL PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    pressure DECIMAL(6,2),
    wind_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX date_time ON weather_predict_1d(date_time);

DROP TABLE IF EXISTS weather_predict_3d;
CREATE TABLE weather_predict_3d (
    id SERIAL PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    pressure DECIMAL(6,2),
    wind_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS weather_predict_6d;
CREATE TABLE weather_predict_6d (
    id SERIAL PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    pressure DECIMAL(6,2),
    wind_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);