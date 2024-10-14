import ballerinax/kafka;
import ballerina/io;
import ballerinax/mysql;
import ballerina/sql;
import ballerina/lang.runtime;
import ballerina/os;


// Configuration using environment variables
configurable string kafkaHost = os:getEnv("KAFKA_HOST");
configurable string dbHost = os:getEnv("DB_HOST");
configurable string dbUser = os:getEnv("DB_USER");
configurable string dbPassword = os:getEnv("DB_PASSWORD");
configurable int dbPort = 3306;
configurable string dbName = os:getEnv("DB_NAME");

// Kafka configurations
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "logistics-group",
    topics: ["logistics-to-standard", "logistics-to-express", "logistics-to-international"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
};

// Database connection with retry mechanism
function initDatabase() returns mysql:Client|error {
    mysql:Options mysqlOptions = {
        ssl: { mode: mysql:SSL_PREFERRED },
        connectTimeout: 10
    };
    return new (host = dbHost, user = dbUser, password = dbPassword, 
                port = dbPort, database = dbName, options = mysqlOptions);
}

function connectWithRetry() returns error? {
    int retryCount = 0;
    int maxRetries = 5;
    while (retryCount < maxRetries) {
        var result = initDatabase();
        if (result is mysql:Client) {
            io:println("Successfully connected to the database");
            return;
        } else {
            io:println("Failed to connect to the database. Retrying in 5 seconds...");
            retryCount += 1;
            if (retryCount == maxRetries) {
                return error("Max retries reached. Unable to connect to the database.");
            }
            runtime:sleep(5);
        }
    }
    return error("Unexpected error occurred during database connection");
}

public function main() returns error? {
    // Initialize database and Kafka components
    check connectWithRetry();
    mysql:Client dbClient = check initDatabase();
    kafka:Consumer consumer = check new (kafkaHost, consumerConfig);
   
    io:println("Logistics service started.");

    while true {
        kafka:ConsumerRecord[] records = check consumer->poll(1);
        foreach var kafkaRecord in records {
            byte[] messageContent = kafkaRecord.value;
           
            json|error deliveryRequest = check string:fromBytes(messageContent);
           
            if deliveryRequest is error {
                io:println("Error parsing delivery request: ", deliveryRequest.message());
                continue;
            }
           
            string shipmentId = check deliveryRequest.shipmentId.ensureType();
            string firstName = check deliveryRequest.firstName.ensureType();
            string lastName = check deliveryRequest.lastName.ensureType();
            string contactNumber = check deliveryRequest.contactNumber.ensureType();
            string deliveryType = check deliveryRequest.deliveryType.ensureType();
            string pickupLocation = check deliveryRequest.pickupLocation.ensureType();
            string deliveryLocation = check deliveryRequest.deliveryLocation.ensureType();
            string preferredPickupTime = check deliveryRequest.preferredPickupTime.ensureType();
            string preferredDeliveryTime = check deliveryRequest.preferredDeliveryTime.ensureType();

            // Insert new customer and retrieve the generated customer_id
            sql:ExecutionResult|sql:Error customerResult = dbClient->execute(
                `INSERT INTO customers (first_name, last_name, contact_number) 
                 VALUES (${firstName}, ${lastName}, ${contactNumber})`
            );

            if customerResult is sql:Error {
                io:println("Error inserting customer data: ", customerResult.message());
                continue;
            }
 // Retrieve the newly inserted customer ID
anydata|sql:Error customerRow = dbClient->queryRow(
    `SELECT customer_id FROM customers
     WHERE first_name = ${firstName} AND last_name = ${lastName} 
     AND contact_number = ${contactNumber}`
);
if customerRow is sql:Error {
    io:println("Error retrieving customer ID: ", customerRow.message());
    return;
}
     int customerId;
if customerResult.lastInsertId is int {
    customerId = <int>customerResult.lastInsertId;
} else {
    // If lastInsertId is null, it means the customer already existed
    // We need to retrieve the existing customer's ID
    sql:ParameterizedQuery query = `SELECT customer_id FROM customers WHERE first_name = ${firstName} AND last_name = ${lastName}`;
    stream<record {int customer_id;}, sql:Error?> resultStream = dbClient->query(query);
    record {|record {int customer_id;}? value;|}? result = check resultStream.next();
    check resultStream.close();

    if result is record {|record {int customer_id;} value;|} {
        customerId = result.value.customer_id;
    } else {
        io:println("Error: Unable to retrieve customer ID");
        return;
    }
}

            // Insert shipment record
            sql:ExecutionResult|sql:Error shipmentResult = dbClient->execute(
                `INSERT INTO shipments (
                    shipment_id, customer_id, delivery_type, pickup_location, 
                    delivery_location, preferred_pickup_time, preferred_delivery_time, status
                 ) VALUES (
                    ${shipmentId}, ${customerId}, ${deliveryType}, ${pickupLocation}, 
                    ${deliveryLocation}, ${preferredPickupTime}, ${preferredDeliveryTime}, 'pending'
                 )`
            );

            if shipmentResult is sql:Error {
                io:println("Error inserting shipment data: ", shipmentResult.message());
                continue;
            }

            io:println("Shipment inserted successfully with ID: ", shipmentId);
        }
    }
}
