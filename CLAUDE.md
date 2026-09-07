# Store IoT System Architecture & Rules

## System Overview
- **Edge Devices**: Atom Lite (ESP32) inside stores with IR transmitter & sensors.
- **Edge Gateway**: Store Management PC running local Mosquitto + SQLite WAL + HTTP OTA Server (Port 8080).
- **Cloud Backend**: FastAPI + WebSocket server for real-time live streaming & statistics.
- **Frontend**: Responsive Hybrid Web (Mobile-first + Desktop view) with Chart.js & Tailwind CSS.

## Key Constraints & Protocols
1. **Sensor Packet (Atom -> PC)**: 12-byte packed C-struct (0xAA header, 1B devId, 2B seq, 2B temp*100, 2B hum*100, 2B light, 1B flags, 1B checksum).
2. **AC Control Packet (PC -> Atom)**: 8-byte packed C-struct (0x55 header, 1B targetId, 1B power, 1B mode, 1B temp, 1B fan, 1B chk, 0xEE tail).
3. **Store Auth & Dynamic 30-Day Grace Period**: Daily check via `POST /api/v1/store/authorize`. Dynamic configurable grace period (default 30 days) with local config persistence (`store_license_config.json`).
4. **SOTA Pipeline**: Base firmware (sensor + OTA) deployed first -> Web AC model selection -> Local fast HTTP OTA (5s flashing) -> AC IR control enabled (`IRremoteESP8266`).
5. **Storage & Live Stream Policy**: 
   - 1-second raw sensor data kept in local SQLite (WAL mode) for 1 year (~5GB).
   - 1-minute downsampled stats sent to Cloud periodically.
   - **On-Demand Live Stream**: When an external user opens the PC/Mobile web (WebSocket connected), Store PC streams 1-second live data directly to the web UI in real-time (<100ms latency).