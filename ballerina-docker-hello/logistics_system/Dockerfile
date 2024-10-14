FROM ballerina/ballerina:2201.7.2

WORKDIR /home/ballerina

COPY --chown=ballerina:ballerina . .

USER ballerina

RUN bal build

# List contents of target/bin for debugging
RUN ls -l target/bin

ARG SERVICE_NAME

CMD bal run target/bin/logistics_system.jar