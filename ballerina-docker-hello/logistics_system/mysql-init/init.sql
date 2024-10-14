CREATE TABLE IF NOT EXISTS customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    contact_number VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id VARCHAR(36) PRIMARY KEY,
    customer_id INT,
    delivery_type ENUM('standard', 'express', 'international') NOT NULL,
    pickup_location VARCHAR(255) NOT NULL,
    delivery_location VARCHAR(255) NOT NULL,
    preferred_pickup_time DATETIME,
    preferred_delivery_time DATETIME,
    status ENUM('pending', 'in_transit', 'delivered') DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);