import ballerina/io;
import ballerinax/kafka;
import ballerina/uuid;


configurable string kafkaHost = "localhost:9092";

public function runClient() returns error? {
    kafka:ProducerConfiguration producerConfig = {
        clientId: "logistics-client",
        acks: "all",
        retryCount: 3
    };
    kafka:Producer kafkaProducer = check new (kafkaHost, producerConfig);
    
    io:println("Connected to Kafka at " + kafkaHost);
    
    while true {
        io:println("\n--- Logistics System Client ---");
        io:println("Enter delivery details (or type 'quit' to exit):");
        string deliveryType = io:readln("Delivery Type (standard/express/international): ");
        if (deliveryType == "quit") {
            break;
        }
        
        if (deliveryType != "standard" && deliveryType != "express" && deliveryType != "international") {
            io:println("Invalid delivery type. Please enter standard, express, or international.");
            continue;
        }
        
        json payload = {
            "shipmentId": uuid:createType1AsString(),
            "deliveryType": deliveryType,
            "firstName": io:readln("First Name: "),
            "lastName": io:readln("Last Name: "),
            "contactNumber": io:readln("Contact Number: "),
            "pickupLocation": io:readln("Pickup Location: "),
            "deliveryLocation": io:readln("Delivery Location: "),
            "preferredPickupTime": io:readln("Preferred Pickup Time (YYYY-MM-DD HH:MM:SS): "),
            "preferredDeliveryTime": io:readln("Preferred Delivery Time (YYYY-MM-DD HH:MM:SS): ")
        };
        
        string topic = "logistics-to-" + deliveryType;
        
        kafka:ProducerRecord producerRecord = {
            topic: topic,
            value: payload.toJsonString().toBytes()
        };
        
        kafka:Error? sendResult = kafkaProducer->send(producerRecord);
        
        if sendResult is kafka:Error {
            io:println("Error sending message: " + sendResult.message());
        } else {
            io:println("\nDelivery request sent successfully to topic: " + topic);
            io:println("Details:");
            io:println(payload.toJsonString());
        }
    }
    io:println("Client terminated. Goodbye!");
}