# atom-air

Store IoT system: ATOM Lite (ESP32) units in stores report environment data and
control air conditioners over IR; a Go gateway on the store PC bridges them to a
FastAPI cloud with a hybrid web UI.

```
ATOM Lite  <--MQTT-->  gateway (Go, store PC)  <--WebSocket-->  cloud (FastAPI)  <-->  browser
firmware/              gateway/                                 cloud/
```

| directory | contents |
|---|---|
| [`firmware/`](firmware/README.md) | PlatformIO firmware for the real M5Stack ATOM Lite (base + SOTA AC image) |
| [`gateway/`](gateway/README.md) | store gateway: Mosquitto bridge, SQLite WAL, HTTP OTA server, Windows service |
| [`cloud/`](cloud/README.md) | cloud backend: live streaming, statistics, licensing, web UI + admin console |
| [`common/`](common/) | wire protocol — single source of truth, shared test vectors |
| [`tools/`](tools/README.md) | mock stack: fake devices + one-command local run without hardware |
