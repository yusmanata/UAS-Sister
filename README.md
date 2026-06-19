# UAS Sistem Terdistribusi - Pub-Sub Log Aggregator

Sistem ini mengimplementasikan Pub-Sub log aggregator multi-service dengan Docker Compose.
Mendukung idempotency, deduplikasi kuat, dan transaksi/kontrol konkurensi (mencegah race condition).

## Arsitektur
1. **Aggregator (`FastAPI`)**: Menyediakan endpoint API untuk publish log dan melihat statistik/event.
2. **Consumer (`Python Worker`)**: Membaca event dari Redis Stream dan menyimpannya ke PostgreSQL secara atomik. Berjalan independen dari web server, memungkinkan di-scale.
3. **Broker (`Redis`)**: Menangani antrian log berbasis Pub/Sub menggunakan *Redis Streams*.
4. **Storage (`PostgreSQL`)**: Database relasional untuk penyimpanan data persisten dengan jaminan transaksional.
5. **Publisher**: Layanan simulator yang mem-publish ribuan log termasuk duplikat untuk membuktikan deduplikasi berfungsi.

## Fitur Utama
- **Idempotency & Deduplication**: PostgreSQL memanfaatkan `UNIQUE(topic, event_id)` dan query `INSERT ... ON CONFLICT DO NOTHING`. Meskipun puluhan consumer menerima log duplikat secara serentak, Postgres memastikan secara atomik bahwa hanya 1 entri log unik yang tersimpan.
- **Transaksi & Konkurensi**: Update metrik statistik (`received`, `unique_processed`, `duplicate_dropped`) dilakukan menggunakan query atomik `UPDATE stats SET count = count + X` di dalam satu transaksi (Transaction Boundary) untuk mencegah fenomena *lost-updates* di lingkungan multi-worker.
- **Isolation Level**: Menggunakan level `READ COMMITTED` (standar PostgreSQL). Di tingkat ini, mitigasi *Unique Index* dan query spesifik `ON CONFLICT` sudah sepenuhnya melindungi data dari *race condition* penambahan data duplikat maupun lost update statistik. Kita terhindar dari pemborosan performa *lock* yang sangat masif jika harus menggunakan level isolasi `SERIALIZABLE`.
- **Reliability & Ordering**: Sistem ini menjamin *At-least-once delivery* dari publisher. Karena merupakan log aggregator terdistribusi, menjamin *strict total ordering* di seluruh sistem akan mengorbankan skalabilitas secara signifikan. Oleh karena itu, kita tidak mensyaratkan total ordering mutlak, melainkan mempraktikkan **Eventual Consistency** dan **Partial Ordering** (berdasarkan timestamp log internal dari asal *source* masing-masing). Konsumen memproses sedapat mungkin sesuai urutan di *Redis Stream*, namun hasil akhir (pengurutan event) diselesaikan pada saat *query database* (contoh: `ORDER BY timestamp DESC` pada saat query analitik) dan bukan di tingkat transport atau antrian consumer.

## Cara Menjalankan

1. Build dan jalankan seluruh services (berjalan di background):
   ```bash
   docker compose up -d --build
   ```

2. Anda bisa men-scale up layanan *consumer* untuk memverifikasi ketahanan sistem menangani multi-thread konkurensi:
   ```bash
   docker compose up -d --scale consumer=3
   ```

3. Pantau log performa dan perilaku duplikasinya:
   ```bash
   docker compose logs -f aggregator consumer publisher
   ```

## API Endpoint (http://localhost:8080)
- `POST /publish` - Menerima log. Payload JSON: 
  `{"topic": "...", "event_id": "...", "timestamp": "...", "source": "...", "payload": {}}`
- `GET /stats` - Mengembalikan *stats performance* yang meliputi total received, unique_processed, dan duplicate_dropped.
- `GET /events` - Melihat list event log unik yang berhasil di-commit.

## Menjalankan Automated Unit Tests
Testing menggunakan Pytest. Memerlukan environment (bisa dijalankan via shell python local atau virtual environment):
```bash
pip install -r aggregator/requirements.txt
pip install httpx pytest pytest-asyncio
pytest tests/ -v -s
```
