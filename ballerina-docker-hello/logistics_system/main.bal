import ballerina/io;
import ballerina/lang.runtime;
import logistics_system.logistics_service;
import logistics_system.standard_delivery_service;
import logistics_system.express_delivery_service;
import logistics_system.international_delivery_service;
import logistics_system.clients;

public function main(string... args) returns error? {
    if args.length() > 0 && args[0] == "client" {
        io:println("Starting client...");
        check clients:runClient();
        io:println("Client finished.");
    } else {
        io:println("Starting all services...");
        
        _ = start logistics_service:main();
        _ = start standard_delivery_service:main();
        _ = start express_delivery_service:main();
        _ = start international_delivery_service:main();
        
        io:println("All services started. Services will run until manually stopped.");
        
        // Keep the main thread running
        while true {
            runtime:sleep(60);
        }
    }
}