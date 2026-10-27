# Lexy Hub Logs API Pro

## Endpoints
- `GET /` — panel visual en vivo.
- `GET /health` — health check de Railway.
- `POST /recibelogs` — recibe un log JSON.
- `GET /logs?limit=100` — logs activos en JSON.
- `GET /stats` — estadísticas básicas.

## Duplicados
Se descarta la combinación `job_id + name + weight + rarity` durante 30 segundos (configurable), ignorando mayúsculas y espacios exteriores. El mismo Job ID puede repetirse si cambia el nombre, el peso o la rareza. Los registros activos desaparecen de `/logs` después de 2 segundos por defecto.

## Railway
1. Sube estos archivos a GitHub y crea un proyecto desde el repositorio.
2. Configura `API_KEY` en Variables con una clave larga y privada.
3. Despliega y abre el dominio público.
4. En el panel, pega la API key y pulsa Conectar.
5. Para conservar SQLite entre despliegues, monta un volumen persistente en `/app/data` y define `DATABASE_PATH=/app/data/logs.sqlite3`.

## Ejemplo
```bash
curl -X POST "https://TU-DOMINIO/recibelogs" \
 -H "Content-Type: application/json" -H "X-API-Key: TU_API_KEY" \
 -d '{"name":"Starry Fox","generation":"791.1m","generation_raw":791100000,"weight":"2.5 tons","rarity":"Secret","zone":"Enchanted Forest","player_count":8,"image_url":"https://example.com/image.png","join_url":"https://www.roblox.com/games/start?placeId=123&gameInstanceId=abc","place_id":123,"job_id":"abc"}'
```

## Variables
- `API_KEY`: clave para proteger endpoints; configúrala en producción.
- `LOG_TTL_SECONDS`: tiempo visible, predeterminado 2 segundos.
- `DEDUP_TTL_SECONDS`: ventana antiduplicados, predeterminado 30 segundos.
- `MAX_ACTIVE_LOGS`: máximo de logs activos.
- `RATE_LIMIT_PER_MINUTE`: límite básico por IP.
- `DATABASE_PATH`: ruta de SQLite.

La interfaz consulta cada 400 ms, pero latencia o red lenta pueden hacer que un log muy breve no llegue a mostrarse. SQLite es adecuado para una instancia pequeña; para varias réplicas o alto tráfico, conviene Redis y rate limiting distribuido. Usa solo datos que tengas autorización para recopilar y no publiques la API key.
