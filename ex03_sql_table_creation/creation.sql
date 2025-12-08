-- Dimension Tables

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    rate_code_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    payment_type_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT PRIMARY KEY,
    borough VARCHAR(255),
    zone VARCHAR(255),
    service_zone VARCHAR(255)
);

-- Fact Table

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    rate_code_id INT,
    store_and_fwd_flag CHAR(1),
    pulocation_id INT,
    dolocation_id INT,
    payment_type_id INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    FOREIGN KEY (vendor_id) REFERENCES dim_vendor(vendor_id),
    FOREIGN KEY (rate_code_id) REFERENCES dim_rate_code(rate_code_id),
    FOREIGN KEY (payment_type_id) REFERENCES dim_payment_type(payment_type_id),
    FOREIGN KEY (pulocation_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (dolocation_id) REFERENCES dim_location(location_id)
);
