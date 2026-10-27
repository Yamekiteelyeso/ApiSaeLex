from __future__ import annotations
import asyncio, hashlib, json, logging, os, sqlite3, time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse
from typing import Any
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

APP_NAME = "Lexy Hub Logs API"
VERSION = "2.0.0"
DB_PATH = Path(os.getenv("DATABASE_PATH", "./data/logs.sqlite3"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_TTL = max(1.0, min(float(os.getenv("LOG_TTL_SECONDS", "2")), 300.0))
DEDUP_TTL = max(LOG_TTL, min(float(os.getenv("DEDUP_TTL_SECONDS", "30")), 86400.0))
API_KEY = os.getenv("API_KEY", "").strip()
MAX_ACTIVE = max(100, min(int(os.getenv("MAX_ACTIVE_LOGS", "5000")), 100000))
RATE_LIMIT = max(10, min(int(os.getenv("RATE_LIMIT_PER_MINUTE", "120")), 10000))
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("LexyHubLogs")

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
        """)

class LogPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)
    name: str = Field(min_length=1, max_length=200)
    generation: str | None = Field(default=None, max_length=80)
    generation_raw: int | float | None = None
    weight: str = Field(min_length=1, max_length=100)
    rarity: str = Field(min_length=1, max_length=100)
    zone: str | None = Field(default=None, max_length=200)
    player_count: int | None = Field(default=None, ge=0, le=10000)
    image_url: str | None = Field(default=None, max_length=2048)
    join_url: str | None = Field(default=None, max_length=2048)
    place_id: int | str | None = None
    job_id: str = Field(min_length=1, max_length=200)

    @field_validator("name", "weight", "rarity", "job_id")
    @classmethod
    def nonblank(cls, v):
        if not v.strip(): raise ValueError("Este campo no puede estar vacío")
        return v.strip()

    @field_validator("image_url", "join_url")
    @classmethod
    def http_url(cls, v):
        if v is None or v == "": return None
        p = urlparse(v)
        if p.scheme not in ("http", "https") or not p.netloc:
            raise ValueError("La URL debe usar http o https")
        return v

    @field_validator("generation_raw")
    @classmethod
    def nonnegative_finite(cls, v):
        if v is not None and (v < 0 or (isinstance(v, float) and (v != v or abs(v) == float("inf")))):
            raise ValueError("generation_raw debe ser un número finito no negativo")
        return v

def make_key(d: dict[str, Any]) -> str:
    # Se permiten Job IDs repetidos; solo se descarta la combinación exacta de 4 campos.
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

@app.get("/stats")
async def stats(x_api_key: str|None=Header(default=None), authorization: str|None=Header(default=None)):
    auth(x_api_key,authorization); now=time.time()
    with db() as c:
        prune(c,now)
        a=c.execute("SELECT COUNT(*) n FROM logs WHERE expires_at>?",(now,)).fetchone()["n"]
        d=c.execute("SELECT COUNT(*) n FROM dedupe WHERE expires_at>?",(now,)).fetchone()["n"]
    return {"ok":True,"active_logs":a,"dedupe_keys":d,"ttl_seconds":LOG_TTL}


HTML = '<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Lexy Hub · Live Logs</title><style>\n:root{color-scheme:dark;--bg:#080b12;--panel:#111827;--line:#253148;--text:#edf3ff;--muted:#9aa9c0;--accent:#60a5fa}*{box-sizing:border-box}body{margin:0;background:radial-gradient(ellipse at top,#14223a 0,var(--bg) 55%);font:14px/1.5 system-ui,Segoe UI,sans-serif;color:var(--text)}main{max-width:1120px;margin:auto;padding:24px 16px 50px}.top{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;margin-bottom:20px}h1{font-size:24px;margin:0}.sub{color:var(--muted);margin-top:4px}.status{border:1px solid var(--line);border-radius:999px;padding:7px 11px;color:#b9f6ca;background:#0c2019}.toolbar{display:flex;gap:8px;flex-wrap:wrap;margin:16px 0}.toolbar input{flex:1;min-width:180px}input,button{border:1px solid var(--line);border-radius:10px;background:#0c1220;color:var(--text);padding:10px 12px;font:inherit}button{cursor:pointer;background:#17345b;border-color:#28558c}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}.card{background:linear-gradient(150deg,#141e30,#0e1420);border:1px solid var(--line);border-radius:16px;overflow:hidden}.hero{height:130px;background:#0a101b;display:flex;align-items:center;justify-content:center;overflow:hidden}.hero img{width:100%;height:100%;object-fit:cover}.body{padding:14px}.name{font-size:18px;font-weight:750;display:flex;justify-content:space-between;gap:8px}.rarity{font-size:11px;padding:4px 7px;border-radius:7px;background:#312453;color:#e2d5ff;white-space:nowrap;height:max-content}.stats{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:12px 0}.stat{background:#0b1220;border:1px solid #1d2a3e;padding:8px;border-radius:9px}.label{color:var(--muted);font-size:11px}.value{font-weight:650;overflow-wrap:anywhere}.foot{display:flex;justify-content:space-between;gap:8px;color:var(--muted);font-size:11px}.join{display:inline-block;text-decoration:none;color:#bfdbfe;border:1px solid #2a4e7b;padding:7px 10px;border-radius:8px;margin-top:10px}.empty{border:1px dashed var(--line);border-radius:14px;padding:40px 18px;text-align:center;color:var(--muted);grid-column:1/-1}.error{color:#fca5a5}</style></head><body><main><div class="top"><div><h1>Lexy Hub <span style="color:var(--accent)">Live Logs</span></h1><div class="sub">Eventos en vivo · expiración automática</div></div><div id="status" class="status">● Desconectado</div></div><div class="toolbar"><input id="key" type="password" placeholder="API Key (solo esta pestaña)"><button id="connect">Conectar</button><input id="filter" placeholder="Filtrar nombre, rareza, zona o Job ID"></div><div class="toolbar"><span class="sub" id="summary">0 logs activos</span><span class="sub" id="updated"></span></div><div id="grid" class="grid"><div class="empty">Introduce tu API Key y pulsa Conectar.</div></div></main><script>\nconst $=s=>document.querySelector(s),grid=$(\'#grid\'),status=$(\'#status\');let key=sessionStorage.getItem(\'lexy_api_key\')||\'\',timer=null;$(\'#key\').value=key;\nfunction esc(v){return String(v??\'\').replace(/[&<>"\']/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\',"\'":\'&#39;\'}[c]))}\nfunction card(x){let img=x.image_url?`<img loading="lazy" src="${esc(x.image_url)}" alt="" onerror="this.remove()">`:\'\';let join=x.join_url&&/^https?:\\/\\//i.test(x.join_url)?`<a class="join" href="${esc(x.join_url)}" target="_blank" rel="noopener noreferrer">Abrir partida ↗</a>`:\'\';return `<article class="card"><div class="hero">${img}</div><div class="body"><div class="name"><span>${esc(x.name)}</span><span class="rarity">${esc(x.rarity)}</span></div><div class="stats"><div class="stat"><div class="label">Generación</div><div class="value">${esc(x.generation||x.generation_raw||\'—\')}</div></div><div class="stat"><div class="label">Peso</div><div class="value">${esc(x.weight)}</div></div><div class="stat"><div class="label">Zona</div><div class="value">${esc(x.zone||\'—\')}</div></div><div class="stat"><div class="label">Jugadores</div><div class="value">${esc(x.player_count??\'—\')}</div></div></div><div class="foot"><span>Job: ${esc((x.job_id||\'\').slice(0,18))}</span><span>${Number(x.expires_in||0).toFixed(1)}s</span></div>${join}</div></article>`}\nasync function poll(){if(!key)return;try{let r=await fetch(\'/logs?limit=200\',{headers:{\'X-API-Key\':key},cache:\'no-store\'});if(r.status===401){status.textContent=\'● API Key incorrecta\';status.className=\'status error\';return}if(!r.ok)throw Error(\'HTTP \'+r.status);let d=await r.json(),q=$(\'#filter\').value.toLowerCase();let items=(d.logs||[]).filter(x=>[x.name,x.rarity,x.zone,x.job_id].join(\' \').toLowerCase().includes(q));grid.innerHTML=items.length?items.map(card).join(\'\'):\'<div class="empty">No hay logs activos que coincidan.</div>\';$(\'#summary\').textContent=`${d.count} logs activos`;$ (\'#updated\').textContent=\'Actualizado \'+new Date().toLocaleTimeString();status.textContent=\'● Conectado\';status.className=\'status\'}catch(e){status.textContent=\'● Error de conexión\';status.className=\'status error\'}}\n$(\'#connect\').onclick=()=>{key=$(\'#key\').value.trim();sessionStorage.setItem(\'lexy_api_key\',key);if(timer)clearInterval(timer);poll();timer=setInterval(poll,400)};$(\'#filter\').addEventListener(\'input\',poll);if(key)$(\'#connect\').click();\n</script></body></html>'
