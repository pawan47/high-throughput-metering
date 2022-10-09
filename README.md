# high-throughput-metering
This Repo implements high through put metering service

## How to run server

```bash
cd meteringsrv
go run main.go
```

## API endpoints
```
- GET /meter
query params:
id: customer id 
time_epoch_greater: parameter to query for timerange query
time_epoch_less: parameter to query for timerange query

ex: /meter?id=1&time_epoch_greater=1&time_epoch_less=1
```

```
- POST /meter
Body:
{
    "id": "1",
    "bytes": 3,
    "meter_time_epoch": 2
}

```

### NOTE:
- Please ask for creds for testing purpose, creds are wrong in repo
- Please use postman collection for testing purposes
