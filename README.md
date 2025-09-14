# Distribution System Assignment 2
## Overview
This assignment implements a **distributed weather aggregation system** with 3 main components 
- **Aggregation Server**
  Collects weather updates from content servers, stores them with a WAL, and serves aggregated weather data to client.
- **Content Server**
  Reads local weather data files, converts them into JSON, and uploads them to the aggregation server using `PUT`.
- **GET Client**
  Sends `GET` requests to the aggregation server to retrieve the current aggregated weather feed and prints in readable format.
--- 
## Features 
- **Lamport Clock**
  All requests/responses include `X-Lamport` headers to make sure ordering across multiple clients and servers.
- **Expiry Sweeper**
  Removes content from servers that have not communicated within 30s.
- **Failure Handling**
  - `GETClient` and `ContentServer` retry automatically on connection errors.
  - Invalid requests return through HTTP status codes.
---

## Build and Run


### Compile
```bash
javac -cp "lib/*" -d out $(find src -name "*.java")
```

### Start Server
```bash
java -cp out agg.AggregationServer 4567
```

### Run Content Server
For the Content Server, you have to open a new terminal
```bash
java -cp out client.ContentServer localhost:4567 Adelaide.txt
```
Or you can open another new terminal to test more than one Content Server
```bash
java -cp out client.ContentServer localhost:4567 Sydeny.txt
```
### Run GET Client 
```bash
java -cp out client.GETClient localhost:4567
```
That's how you can run the content server.
here is the exmaple of Adelaide.txt, in case you want to input more file to test:
```bash
id:IDS60901
name:Adelaide (West Terrace / ngayirdapira)
state:SA
time_zone:CST
lat:-34.9
lon:138.6
local_date_time:15/04:00pm
local_date_time_full:20230715160000
air_temp:13.3
apparent_t:9.5
cloud:Partly cloudy
dewpt:5.7
press:1023.9
rel_hum:60
wind_dir:S
wind_spd_kmh:15
wind_spd_kt:8
```
### Testing
This project included Junit Tests function, it can help to test all the files in a simple step.
Complie test file:
```bash
javac -cp "out:lib/*" -d out src/test/AggregationServerTest.java
```
Run tests:
```bash
java -jar lib/junit-platform-console-standalone-1.10.5.jar \
  -cp out --scan-class-path
```
### Test Coverage
- First PUT return 201 Created
- Subsequent PUT returns 200 OK
- GET returns stored data
- Empty PUT returns 204 NO Content
- Invalid JSON returns 500
- Bad method returns 400
- Expired data remvoed after 30s
- WAL recovery after crash
- Lamport Clock is monotonic

### Notes
- Default port is 4567
- WAL File(wal.log) in project root.
  
