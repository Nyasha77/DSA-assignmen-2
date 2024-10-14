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
kafka:ProducerConfiguration producerConfig = {
    clientId: "logistics-producer"
};

kafka:ConsumerConfiguration consumerConfig = {
    groupId: "logistics-response-group",
    topics: ["delivery-responses"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
};

// Database connection function with retry mechanism
function initDatabase() returns mysql:Client|error {
    mysql:Options mysqlOptions = {
        ssl: {
            mode: mysql:SSL_PREFERRED
        },
        connectTimeout: 10
    };
    return new (host = dbHost, user = dbUser, password = dbPassword, port = dbPort, database = dbName, options = mysqlOptions);
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
    // Initialize database connection
    check connectWithRetry();
    mysql:Client dbClient = check initDatabase();

    // Initialize Kafka consumer
    kafka:Consumer consumer = check new (kafkaHost, consumerConfig);
   
    io:println("Logistics service started.");

    while true {
        kafka:ConsumerRecord[] records = check consumer->poll(1);
        foreach var kafkaRecord in records {
            byte[] messageContent = kafkaRecord.value;
            io:println("Received delivery response: ", string:fromBytes(messageContent));
           
            json|error deliveryResponse = check string:fromBytes(messageContent);
            if deliveryResponse is error {
                io:println("Error parsing delivery response: ", deliveryResponse.message());
                continue;
            }

            string shipmentId = check deliveryResponse.shipmentId.ensureType();
            string status = check deliveryResponse.status.ensureType();
            string estimatedDeliveryTime = check deliveryResponse.estimatedDeliveryTime.ensureType();

            // Update shipment status and estimated delivery time
            sql:ExecutionResult|sql:Error result = dbClient->execute(`
                UPDATE shipments
                SET status = ${status}, estimated_delivery_time = ${estimatedDeliveryTime}
                WHERE shipment_id = ${shipmentId}
            `);

            if result is sql:Error {
                io:println("Error updating shipment: ", result.message());
                continue;
            }
           
            io:println("Updated shipment ", shipmentId, " with status ", status, " and estimated delivery time ", estimatedDeliveryTime);
        }
    }
}