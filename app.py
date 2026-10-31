from __future__ import annotations
import asyncio, base64, hashlib, json, logging, os, re, sqlite3, time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse
from typing import Any
import httpx
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

APP_NAME = "SynexHub Logs API"
VERSION = "2.2.0"
DB_PATH = Path(os.getenv("DATABASE_PATH", "./data/logs.sqlite3"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_TTL = max(1.0, min(float(os.getenv("LOG_TTL_SECONDS", "2")), 300.0))
DEDUP_TTL = max(LOG_TTL, min(float(os.getenv("DEDUP_TTL_SECONDS", "30")), 86400.0))
WEBHOOK_DEDUP_TTL = max(30.0, min(float(os.getenv("WEBHOOK_DEDUP_TTL_SECONDS", "1800")), 86400.0))
ROBO_DEDUP_TTL = max(30.0, min(float(os.getenv("WEBHOOK_ROBO_DEDUP_TTL_SECONDS", "300")), 86400.0))
API_KEY = os.getenv("API_KEY", "").strip()
MAX_ACTIVE = max(100, min(int(os.getenv("MAX_ACTIVE_LOGS", "5000")), 100000))
RATE_LIMIT = max(10, min(int(os.getenv("RATE_LIMIT_PER_MINUTE", "120")), 10000))
PRESENCE_TTL = max(10.0, min(float(os.getenv("PRESENCE_TTL_SECONDS", "30")), 600.0))
WEBHOOK_SPAWN_URL = os.getenv("WEBHOOK_SPAWN_URL", "https://discord.com/api/webhooks/1564772572908888114/07GkiIRIkg3LD9f5PhorOLPWtNyqrPzq_WxIAgOkx35KEXqSqXJ4dqmgS9m2D3pJDYFq").strip()
WEBHOOK_ROBO_URL = os.getenv("WEBHOOK_ROBO_URL", "https://discord.com/api/webhooks/1564773472901791867/Xl50toxA5Z9kCyaIugT_0bAxX95UxYXq2QT0rA3slJbKwor5BltUCINd--qpXqNLSvzk").strip()
TRANSPARENT_PNG = bytes([137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 128, 0, 0, 0, 128, 8, 6, 0, 0, 0, 195, 62, 97, 203, 0, 0, 0, 86, 73, 68, 65, 84, 120, 218, 237, 193, 49, 1, 0, 0, 0, 194, 160, 245, 79, 109, 12, 31, 160, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 224, 111, 0, 143, 0, 1, 138, 249, 130, 62, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130])
ROBLOX_USER_RE = re.compile(r"^[A-Za-z0-9_]{3,20}$")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("SynexHubLogs")

def db():
    c = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=10000")
    return c

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY AUTOINCREMENT,dedupe_key TEXT NOT NULL,payload_json TEXT NOT NULL,received_at REAL NOT NULL,expires_at REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS logs_exp ON logs(expires_at);
        CREATE TABLE IF NOT EXISTS dedupe(dedupe_key TEXT PRIMARY KEY,expires_at REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS dedupe_exp ON dedupe(expires_at);
        CREATE TABLE IF NOT EXISTS rates(client_key TEXT PRIMARY KEY,window_start REAL NOT NULL,count INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS presence(user TEXT PRIMARY KEY,last_seen REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS presence_seen ON presence(last_seen);
        """)

class UserPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)
    user: str = Field(min_length=3, max_length=20)
    action: str = Field(default="heartbeat")

    @field_validator("user")
    @classmethod
    def valid_user(cls, v):
        if not ROBLOX_USER_RE.match(v):
            raise ValueError("user inválido")
        return v

    @field_validator("action")
    @classmethod
    def valid_action(cls, v):
        if v not in ("heartbeat", "leave"):
            raise ValueError("action inválida")
        return v

class LogPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)
    name: Any = Field(default="Unknown")
    generation: Any = Field(default=None)
    generation_raw: Any = Field(default=None)
    weight: Any = Field(default="0")
    rarity: Any = Field(default="Common")
    zone: Any = Field(default=None)
    player_count: Any = Field(default=None)
    image_url: Any = Field(default=None)
    join_url: Any = Field(default=None)
    place_id: Any = Field(default=None)
    job_id: Any = Field(default="test_job")

    @field_validator("name", "rarity", "job_id", "weight", "generation", mode="before")
    @classmethod
    def to_string(cls, v):
        if v is None: return ""
        return str(v).strip()

    @field_validator("zone", mode="before")
    @classmethod
    def to_string_optional(cls, v):
        if v is None: return None
        s = str(v).strip()
        return s if s else None

    @field_validator("player_count", mode="before")
    @classmethod
    def to_int(cls, v):
        if v is None: return None
        try: return int(v)
        except (ValueError, TypeError): return None

    @field_validator("image_url", "join_url", mode="before")
    @classmethod
    def safe_url(cls, v):
        if v is None or v == "": return None
        s = str(v).strip()
        p = urlparse(s)
        if p.scheme not in ("http", "https") or not p.netloc:
            return None
        return s

def make_key(d: dict[str, Any]) -> str:
    raw = "\x1f".join(str(d.get(k, "")).strip().casefold() for k in ("job_id","name","weight","rarity"))
    return hashlib.sha256(raw.encode()).hexdigest()

def prune(c, now):
    c.execute("DELETE FROM logs WHERE expires_at <= ?", (now,))
    c.execute("DELETE FROM dedupe WHERE expires_at <= ?", (now,))
    c.execute("DELETE FROM rates WHERE window_start < ?", (now-3600,))

def rate(c, client, now):
    row = c.execute("SELECT window_start,count FROM rates WHERE client_key=?", (client,)).fetchone()
    if row is None or now-row["window_start"] >= 60:
        c.execute("INSERT INTO rates VALUES(?,?,1) ON CONFLICT(client_key) DO UPDATE SET window_start=excluded.window_start,count=1", (client,now))
    elif row["count"] >= RATE_LIMIT:
        raise HTTPException(429, "Límite de solicitudes excedido; intenta más tarde")
    else:
        c.execute("UPDATE rates SET count=count+1 WHERE client_key=?", (client,))

def auth(key, authorization=None):
    if not API_KEY: return
    supplied = key or ""
    if authorization and authorization.lower().startswith("bearer "): supplied = authorization[7:].strip()
    if not hashlib.sha256(supplied.encode()).digest() == hashlib.sha256(API_KEY.encode()).digest():
        raise HTTPException(401, "API key inválida o ausente")

async def cleanup_loop():
    while True:
        try:
            now=time.time()
            with db() as c:
                prune(c,now)
                c.execute("DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY received_at DESC LIMIT ?)", (MAX_ACTIVE,))
        except Exception: logger.exception("Fallo en limpieza")
        await asyncio.sleep(.25)

@asynccontextmanager
async def lifespan(app):
    init_db()
    task=asyncio.create_task(cleanup_loop())
    logger.info("%s %s started", APP_NAME, VERSION)
    try: yield
    finally:
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass

app=FastAPI(title=APP_NAME,version=VERSION,lifespan=lifespan)

@app.middleware("http")
async def safety_headers(request, call_next):
    try: response=await call_next(request)
    except Exception:
        logger.exception("Error interno atendiendo %s", request.url.path)
        response=JSONResponse(status_code=500,content={"ok":False,"detail":"Error interno del servidor"})
    response.headers.update({"X-Content-Type-Options":"nosniff","Cache-Control":"no-store","X-Frame-Options":"DENY","Referrer-Policy":"no-referrer"})
    return response

@app.get("/", response_class=HTMLResponse)
async def dashboard(): return HTMLResponse(HTML)

@app.get("/health")
async def health(): return {"ok":True,"service":APP_NAME,"version":VERSION}

@app.post("/recibelogs")
async def receive(payload: LogPayload, request: Request, x_api_key: str|None=Header(default=None), authorization: str|None=Header(default=None)):
    auth(x_api_key,authorization)
    now=time.time(); data=payload.model_dump(); key=make_key(data)
    client=request.client.host if request.client else "unknown"
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            prune(c,now); rate(c,client,now)
            row=c.execute("SELECT expires_at FROM dedupe WHERE dedupe_key=?",(key,)).fetchone()
            if row and row["expires_at"]>now:
                c.commit()
                return {"ok":True,"duplicate":True,"message":"Log duplicado ignorado"}
            count=c.execute("SELECT COUNT(*) n FROM logs WHERE expires_at>?",(now,)).fetchone()["n"]
            if count>=MAX_ACTIVE: c.execute("DELETE FROM logs WHERE id=(SELECT id FROM logs ORDER BY received_at LIMIT 1)")
            c.execute("INSERT INTO logs(dedupe_key,payload_json,received_at,expires_at) VALUES(?,?,?,?)",(key,json.dumps(data,ensure_ascii=False,separators=(",",":")),now,now+LOG_TTL))
            c.execute("INSERT INTO dedupe VALUES(?,?) ON CONFLICT(dedupe_key) DO UPDATE SET expires_at=excluded.expires_at",(key,now+DEDUP_TTL))
            c.commit()
        except HTTPException: c.rollback(); raise
        except Exception:
            c.rollback(); logger.exception("No se pudo guardar log")
            raise HTTPException(500,"No se pudo guardar el log")
    return {"ok":True,"duplicate":False,"message":"Log recibido","ttl_seconds":LOG_TTL}

class WebhookPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    kind: str = Field(default="spawn")
    discord: dict[str, Any]
    attach_png: bool = False
    log: LogPayload | None = None
    dedupe_key: str | None = Field(default=None, max_length=200)

    @field_validator("kind")
    @classmethod
    def valid_kind(cls, v):
        if v not in ("spawn", "robo"):
            raise ValueError("kind inválido")
        return v

async def post_discord(url: str, body: dict[str, Any], attach_png: bool) -> bool:
    """Siempre multipart: adjunta synexhub.png (icono del footer, referenciado como
    attachment://synexhub.png) y, si se pide, transparent.png."""
    payload_json = json.dumps(body, ensure_ascii=False)
    async with httpx.AsyncClient(timeout=15) as http:
        for _ in range(4):
            parts = [("payload_json", (None, payload_json, "application/json"))]
            n = 0
            if attach_png:
                parts.append((f"files[{n}]", ("transparent.png", TRANSPARENT_PNG, "image/png"))); n += 1
            parts.append((f"files[{n}]", ("synexhub.png", LOGO_PNG, "image/png")))
            try:
                res = await http.post(url, files=parts)
            except httpx.HTTPError:
                logger.exception("Fallo de red enviando webhook a Discord")
                await asyncio.sleep(2)
                continue
            if 200 <= res.status_code < 300:
                return True
            if res.status_code == 429:
                await asyncio.sleep(3)
            elif res.status_code >= 500:
                await asyncio.sleep(2)
            else:
                logger.warning("Discord respondió %s: %s", res.status_code, res.text[:200])
                return False
    return False

@app.post("/sendwebhook")
async def send_webhook(payload: WebhookPayload, request: Request, x_api_key: str|None=Header(default=None), authorization: str|None=Header(default=None)):
    """Solo envía a Discord (no guarda logs; eso lo hace /recibelogs).
    spawn: deduplica con clave propia 'wh:' para no repetir el mensaje en Discord.
    robo: envía al webhook de robos; si trae dedupe_key (UserId:uid) también deduplica."""
    auth(x_api_key,authorization)
    logger.info("sendwebhook recibido kind=%s dedupe=%s", payload.kind, bool(payload.dedupe_key))
    if payload.kind == "spawn" and payload.log is None:
        raise HTTPException(422, "Falta 'log' para kind=spawn (se usa para deduplicar)")
    url = WEBHOOK_SPAWN_URL if payload.kind == "spawn" else WEBHOOK_ROBO_URL
    if not url.startswith("https://"):
        raise HTTPException(500, "Webhook no configurado")
    now=time.time(); client=request.client.host if request.client else "unknown"
    key, ttl = None, WEBHOOK_DEDUP_TTL
    if payload.kind == "spawn":
        key = "wh:" + make_key(payload.log.model_dump())
    elif payload.dedupe_key:
        key = "rb:" + hashlib.sha256(payload.dedupe_key.encode()).hexdigest()
        ttl = ROBO_DEDUP_TTL

    # Reserva la clave ANTES de enviar: dos requests iguales a la vez no envían dos veces
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            prune(c,now); rate(c,"webhook:"+client,now)
            if key:
                row=c.execute("SELECT expires_at FROM dedupe WHERE dedupe_key=?",(key,)).fetchone()
                if row and row["expires_at"]>now:
                    c.commit()
                    logger.info("Webhook %s duplicado ignorado", payload.kind)
                    return {"ok":True,"duplicate":True,"sent":False,"message":"Webhook duplicado ignorado"}
                c.execute("INSERT INTO dedupe VALUES(?,?) ON CONFLICT(dedupe_key) DO UPDATE SET expires_at=excluded.expires_at",(key,now+ttl))
            c.commit()
        except HTTPException: c.rollback(); raise
        except Exception:
            c.rollback(); logger.exception("No se pudo reservar la clave de webhook")
            raise HTTPException(500,"Error interno")

    sent = await post_discord(url, payload.discord, payload.attach_png)
    if not sent:
        if key:
            with db() as c:
                c.execute("DELETE FROM dedupe WHERE dedupe_key=?",(key,))
        raise HTTPException(502, "Discord no aceptó el webhook")
    logger.info("Webhook %s enviado a Discord", payload.kind)
    return {"ok":True,"duplicate":False,"sent":True,"message":"Webhook enviado"}

@app.get("/logs")
async def logs(request: Request, limit: int=Query(100,ge=1,le=500), x_api_key: str|None=Header(default=None), authorization: str|None=Header(default=None)):
    auth(x_api_key,authorization)
    now=time.time(); client=request.client.host if request.client else "unknown"
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            prune(c,now); rate(c,"read:"+client,now)
            rows=c.execute("SELECT id,payload_json,received_at,expires_at FROM logs WHERE expires_at>? ORDER BY received_at DESC LIMIT ?",(now,limit)).fetchall()
            c.commit()
        except HTTPException: c.rollback(); raise
    out=[]
    for r in rows:
        try: data=json.loads(r["payload_json"])
        except json.JSONDecodeError: continue
        out.append({"id":r["id"],**data,"received_at":r["received_at"],"expires_at":r["expires_at"],"expires_in":max(0,round(r["expires_at"]-now,2))})
    return {"ok":True,"count":len(out),"ttl_seconds":LOG_TTL,"logs":out}

@app.post("/senduser")
async def send_user(payload: UserPayload, request: Request):
    """heartbeat: registra o refresca al usuario. leave: lo borra al salir del juego.
    Sin API key: el script del cliente lo llama directamente."""
    now=time.time(); client=request.client.host if request.client else "unknown"
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            rate(c,"presence:"+client,now)
            if payload.action=="leave":
                c.execute("DELETE FROM presence WHERE user=?",(payload.user,))
            else:
                c.execute("INSERT INTO presence(user,last_seen) VALUES(?,?) ON CONFLICT(user) DO UPDATE SET last_seen=excluded.last_seen",(payload.user,now))
            c.execute("DELETE FROM presence WHERE last_seen<?",(now-PRESENCE_TTL,))
            c.commit()
        except HTTPException: c.rollback(); raise
    return {"ok":True,"action":payload.action}

@app.get("/users")
async def users(request: Request):
    """Lista de usuarios activos (heartbeat reciente). Sin API key."""
    now=time.time(); client=request.client.host if request.client else "unknown"
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            rate(c,"presence-read:"+client,now)
            c.execute("DELETE FROM presence WHERE last_seen<?",(now-PRESENCE_TTL,))
            rows=c.execute("SELECT user FROM presence ORDER BY user").fetchall()
            c.commit()
        except HTTPException: c.rollback(); raise
    names=[r["user"] for r in rows]
    return {"ok":True,"count":len(names),"users":names,"ttl_seconds":PRESENCE_TTL}

@app.get("/stats")
async def stats(x_api_key: str|None=Header(default=None), authorization: str|None=Header(default=None)):
    auth(x_api_key,authorization); now=time.time()
    with db() as c:
        prune(c,now)
        a=c.execute("SELECT COUNT(*) n FROM logs WHERE expires_at>?",(now,)).fetchone()["n"]
        d=c.execute("SELECT COUNT(*) n FROM dedupe WHERE expires_at>?",(now,)).fetchone()["n"]
    return {"ok":True,"active_logs":a,"dedupe_keys":d,"ttl_seconds":LOG_TTL}

HTML = '<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SynexHub · Live Logs</title><style>\n:root{color-scheme:dark;--bg:#080b12;--panel:#111827;--line:#253148;--text:#edf3ff;--muted:#9aa9c0;--accent:#60a5fa}*{box-sizing:border-box}body{margin:0;background:radial-gradient(ellipse at top,#14223a 0,var(--bg) 55%);font:14px/1.5 system-ui,Segoe UI,sans-serif;color:var(--text)}main{max-width:1120px;margin:auto;padding:24px 16px 50px}.top{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;margin-bottom:20px}h1{font-size:24px;margin:0}.sub{color:var(--muted);margin-top:4px}.status{border:1px solid var(--line);border-radius:999px;padding:7px 11px;color:#b9f6ca;background:#0c2019}.toolbar{display:flex;gap:8px;flex-wrap:wrap;margin:16px 0}.toolbar input{flex:1;min-width:180px}input,button{border:1px solid var(--line);border-radius:10px;background:#0c1220;color:var(--text);padding:10px 12px;font:inherit}button{cursor:pointer;background:#17345b;border-color:#28558c}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}.card{background:linear-gradient(150deg,#141e30,#0e1420);border:1px solid var(--line);border-radius:16px;overflow:hidden}.hero{height:130px;background:#0a101b;display:flex;align-items:center;justify-content:center;overflow:hidden}.hero img{width:100%;height:100%;object-fit:cover}.body{padding:14px}.name{font-size:18px;font-weight:750;display:flex;justify-content:space-between;gap:8px}.rarity{font-size:11px;padding:4px 7px;border-radius:7px;background:#312453;color:#e2d5ff;white-space:nowrap;height:max-content}.stats{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:12px 0}.stat{background:#0b1220;border:1px solid #1d2a3e;padding:8px;border-radius:9px}.label{color:var(--muted);font-size:11px}.value{font-weight:650;overflow-wrap:anywhere}.foot{display:flex;justify-content:space-between;gap:8px;color:var(--muted);font-size:11px}.join{display:inline-block;text-decoration:none;color:#bfdbfe;border:1px solid #2a4e7b;padding:7px 10px;border-radius:8px;margin-top:10px}.empty{border:1px dashed var(--line);border-radius:14px;padding:40px 18px;text-align:center;color:var(--muted);grid-column:1/-1}.error{color:#fca5a5}</style></head><body><main><div class="top"><div><h1>SynexHub <span style="color:var(--accent)">Live Logs</span></h1><div class="sub">Eventos en vivo · expiración automática</div></div><div id="status" class="status">● Desconectado</div></div><div class="toolbar"><input id="key" type="password" placeholder="API Key (solo esta pestaña)"><button id="connect">Conectar</button><input id="filter" placeholder="Filtrar nombre, rareza, zona o Job ID"></div><div class="toolbar"><span class="sub" id="summary">0 logs activos</span><span class="sub" id="updated"></span></div><div id="grid" class="grid"><div class="empty">Introduce tu API Key y pulsa Conectar.</div></div></main><script>\nconst $=s=>document.querySelector(s),grid=$(\'#grid\'),status=$(\'#status\');let key=sessionStorage.getItem(\'synex_api_key\')||\'\',timer=null;$(\'#key\').value=key;\nfunction esc(v){return String(v??\'\').replace(/[&<>"\']/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\',"\'":\'&#39;\'}[c]))}\nfunction card(x){let img=x.image_url?`<img loading="lazy" src="${esc(x.image_url)}" alt="" onerror="this.remove()">`:\'\';let join=x.join_url&&/^https?:\\/\\//i.test(x.join_url)?`<a class="join" href="${esc(x.join_url)}" target="_blank" rel="noopener noreferrer">Abrir partida ↗</a>`:\'\';return `<article class="card"><div class="hero">${img}</div><div class="body"><div class="name"><span>${esc(x.name)}</span><span class="rarity">${esc(x.rarity)}</span></div><div class="stats"><div class="stat"><div class="label">Generación</div><div class="value">${esc(x.generation||x.generation_raw||\'—\')}</div></div><div class="stat"><div class="label">Peso</div><div class="value">${esc(x.weight)}</div></div><div class="stat"><div class="label">Zona</div><div class="value">${esc(x.zone||\'—\')}</div></div><div class="stat"><div class="label">Jugadores</div><div class="value">${esc(x.player_count??\'—\')}</div></div></div><div class="foot"><span>Job: ${esc((x.job_id||\'\').slice(0,18))}</span><span>${Number(x.expires_in||0).toFixed(1)}s</span></div>${join}</div></article>`}\nasync function poll(){if(!key)return;try{let r=await fetch(\'/logs?limit=200\',{headers:{\'X-API-Key\':key},cache:\'no-store\'});if(r.status===401){status.textContent=\'● API Key incorrecta\';status.className=\'status error\';return}if(!r.ok)throw Error(\'HTTP \'+r.status);let d=await r.json(),q=$(\'#filter\').value.toLowerCase();let items=(d.logs||[]).filter(x=>[x.name,x.rarity,x.zone,x.job_id].join(\' \').toLowerCase().includes(q));grid.innerHTML=items.length?items.map(card).join(\'\'):\'<div class="empty">No hay logs activos que coincidan.</div>\';$(\'#summary\').textContent=`${d.count} logs activos`;$ (\'#updated\').textContent=\'Actualizado \'+new Date().toLocaleTimeString();status.textContent=\'● Conectado\';status.className=\'status\'}catch(e){status.textContent=\'● Error de conexión\';status.className=\'status error\'}}\n$(\'#connect\').onclick=()=>{key=$(\'#key\').value.trim();sessionStorage.setItem(\'synex_api_key\',key);if(timer)clearInterval(timer);poll();timer=setInterval(poll,400)};$(\'#filter\').addEventListener(\'input\',poll);if(key)$(\'#connect\').click();\n</script></body></html>'

LOGO_PNG = base64.b64decode("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")
