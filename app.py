import csv
import io
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request


DB_PATH = os.getenv("APP_DB_PATH", "./data.db")
TRACCAR_URL = os.getenv("TRACCAR_SMS_URL", "https://www.traccar.org/sms/")
TRACCAR_TOKEN = os.getenv(
    "TRACCAR_SMS_TOKEN",
    "fbutFSxNRUiBik70R49iPm:APA91bGYHtxERGmY0H2dcgzfjX9NTU0wqtXQ5azJArRClVj3S-bzJmQeoEkV0RCrll10zCnslxRqzAYCF78kvGwIiY84ya_YWivK9pCY2Wxwutu3Jl_-5Qo",
)


app = FastAPI(title="Traccar SMS Manager", version="1.0.0")
templates = Jinja2Templates(directory="templates")

db_lock = threading.Lock()
queue_lock = threading.Lock()


queue_state: dict[str, Any] = {
    "running": False,
    "paused": False,
    "cancelled": False,
    "total": 0,
    "sent": 0,
    "failed": 0,
    "waiting_seconds": 0,
    "last_error": None,
    "current_to": None,
    "started_at": None,
    "finished_at": None,
    "worker_alive": False,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    phone TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT NOT NULL,
                    message TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    status_code INTEGER,
                    response_text TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()


@app.on_event("startup")
def on_startup() -> None:
    init_db()


def sanitize_phone(value: str) -> str | None:
    if not value:
        return None

    raw = value.strip()
    if not raw:
        return None

    has_plus = raw.startswith("+")
    digits = re.sub(r"\D", "", raw)

    if digits.startswith("00"):
        digits = digits[2:]
        has_plus = True

    if not digits:
        return None

    # Foco em padrao BR: garante +55 com DDD (10 ou 11 digitos nacionais).
    if digits.startswith("55") and len(digits) in (12, 13):
        return f"+{digits}"

    if len(digits) in (10, 11):
        return f"+55{digits}"

    if len(digits) in (11, 12) and digits.startswith("0"):
        trimmed = digits.lstrip("0")
        if len(trimmed) in (10, 11):
            return f"+55{trimmed}"

    # Se o usuario informar internacional com + e comprimento valido E.164.
    if has_plus and 8 <= len(digits) <= 15:
        return f"+{digits}"

    return None


def parse_phone_candidates(content: str) -> list[str]:
    values: list[str] = []
    if not content:
        return values

    if "," in content or ";" in content or "\t" in content:
        reader = csv.reader(io.StringIO(content), delimiter=",")
        for row in reader:
            for cell in row:
                if ";" in cell:
                    values.extend(part.strip() for part in cell.split(";"))
                elif "\t" in cell:
                    values.extend(part.strip() for part in cell.split("\t"))
                else:
                    values.append(cell.strip())
    else:
        values.extend(line.strip() for line in content.splitlines())

    return [v for v in values if v]


def send_sms(phone: str, message: str) -> dict[str, Any]:
    headers = {"Authorization": TRACCAR_TOKEN, "Content-Type": "application/json"}
    payload = {"to": phone, "message": message}

    try:
        response = requests.post(TRACCAR_URL, json=payload, headers=headers, timeout=20)
        ok = response.status_code in (200, 202)
        return {
            "success": ok,
            "status_code": response.status_code,
            "response_text": response.text[:4000],
            "error": None if ok else f"HTTP {response.status_code}",
        }
    except requests.RequestException as exc:
        return {
            "success": False,
            "status_code": None,
            "response_text": "",
            "error": f"Falha de conexao: {exc}",
        }


def add_log(
    phone: str,
    message: str,
    success: bool,
    status_code: int | None,
    response_text: str,
    error: str | None,
) -> None:
    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute(
                """
                INSERT INTO logs (phone, message, success, status_code, response_text, error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (phone, message, 1 if success else 0, status_code, response_text, error, utc_now_iso()),
            )
            conn.commit()
        finally:
            conn.close()


def run_queue_worker(recipients: list[str], message: str, interval_seconds: int) -> None:
    with queue_lock:
        queue_state["worker_alive"] = True

    try:
        total = len(recipients)
        for index, phone in enumerate(recipients, start=1):
            with queue_lock:
                if queue_state["cancelled"]:
                    break
                queue_state["current_to"] = phone

            while True:
                with queue_lock:
                    cancelled = queue_state["cancelled"]
                    paused = queue_state["paused"]
                if cancelled:
                    break
                if not paused:
                    break
                time.sleep(0.4)

            with queue_lock:
                if queue_state["cancelled"]:
                    break

            result = send_sms(phone, message)
            add_log(
                phone=phone,
                message=message,
                success=result["success"],
                status_code=result["status_code"],
                response_text=result["response_text"],
                error=result["error"],
            )

            with queue_lock:
                queue_state["sent"] += 1
                if not result["success"]:
                    queue_state["failed"] += 1
                    queue_state["last_error"] = result["error"]

            is_last = index >= total
            if is_last:
                continue

            remaining = max(0, interval_seconds)
            while remaining > 0:
                with queue_lock:
                    if queue_state["cancelled"]:
                        break
                    paused = queue_state["paused"]
                    queue_state["waiting_seconds"] = remaining

                if paused:
                    time.sleep(0.4)
                    continue

                time.sleep(1)
                remaining -= 1

            with queue_lock:
                queue_state["waiting_seconds"] = 0

            with queue_lock:
                if queue_state["cancelled"]:
                    break

    except Exception as exc:
        with queue_lock:
            queue_state["last_error"] = f"Erro interno no worker: {exc}"
    finally:
        with queue_lock:
            queue_state["running"] = False
            queue_state["paused"] = False
            queue_state["worker_alive"] = False
            queue_state["waiting_seconds"] = 0
            queue_state["current_to"] = None
            queue_state["finished_at"] = utc_now_iso()


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/contacts")
def list_contacts() -> list[dict[str, Any]]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT id, name, phone, created_at FROM contacts ORDER BY name COLLATE NOCASE"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


@app.post("/api/contacts")
def create_contact(name: str = Form(...), phone: str = Form(...)) -> dict[str, Any]:
    clean_phone = sanitize_phone(phone)
    if not clean_phone:
        raise HTTPException(status_code=400, detail="Telefone invalido. Informe DDD e numero.")

    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO contacts (name, phone, created_at) VALUES (?, ?, ?)",
                (name.strip() or "Sem nome", clean_phone, utc_now_iso()),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="Contato ja existe na agenda.")
        finally:
            conn.close()

    return {"ok": True, "phone": clean_phone}


@app.post("/api/contacts/import")
async def import_contacts(file: UploadFile = File(...)) -> dict[str, Any]:
    filename = (file.filename or "").lower()
    if not (filename.endswith(".csv") or filename.endswith(".txt")):
        raise HTTPException(status_code=400, detail="Apenas arquivos .csv ou .txt sao aceitos.")

    content_bytes = await file.read()
    try:
        content = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        content = content_bytes.decode("latin-1")

    candidates = parse_phone_candidates(content)
    normalized: list[str] = []
    for candidate in candidates:
        clean = sanitize_phone(candidate)
        if clean:
            normalized.append(clean)

    normalized = sorted(set(normalized))
    return {
        "ok": True,
        "imported": normalized,
        "count": len(normalized),
    }


@app.get("/api/templates")
def list_templates() -> list[dict[str, Any]]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT id, name, content, created_at FROM templates ORDER BY name COLLATE NOCASE"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


@app.post("/api/templates")
def save_template(name: str = Form(...), content: str = Form(...)) -> dict[str, Any]:
    if not content.strip():
        raise HTTPException(status_code=400, detail="Template vazio.")

    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute(
                """
                INSERT INTO templates (name, content, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET content=excluded.content
                """,
                (name.strip() or "Template", content, utc_now_iso()),
            )
            conn.commit()
        finally:
            conn.close()

    return {"ok": True}


@app.get("/api/logs")
def list_logs(limit: int = 100) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 500))
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, phone, message, success, status_code, response_text, error, created_at
            FROM logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


@app.post("/api/queue/start")
def start_queue(
    message: str = Form(...),
    recipients: str = Form(...),
    interval_seconds: int = Form(10),
) -> dict[str, Any]:
    recipient_values = [line.strip() for line in recipients.splitlines() if line.strip()]
    cleaned: list[str] = []
    for value in recipient_values:
        phone = sanitize_phone(value)
        if phone:
            cleaned.append(phone)

    cleaned = list(dict.fromkeys(cleaned))
    if not cleaned:
        raise HTTPException(status_code=400, detail="Nenhum telefone valido para envio.")

    if not message.strip():
        raise HTTPException(status_code=400, detail="Mensagem nao pode ser vazia.")

    wait = max(0, min(interval_seconds, 86400))

    with queue_lock:
        if queue_state["running"]:
            raise HTTPException(status_code=409, detail="Ja existe uma fila em execucao.")

        queue_state.update(
            {
                "running": True,
                "paused": False,
                "cancelled": False,
                "total": len(cleaned),
                "sent": 0,
                "failed": 0,
                "waiting_seconds": 0,
                "last_error": None,
                "current_to": None,
                "started_at": utc_now_iso(),
                "finished_at": None,
                "worker_alive": False,
            }
        )

    worker = threading.Thread(
        target=run_queue_worker,
        args=(cleaned, message, wait),
        daemon=True,
        name="sms-queue-worker",
    )
    worker.start()

    return {"ok": True, "queued": len(cleaned), "interval_seconds": wait}


@app.get("/api/queue/status")
def queue_status() -> dict[str, Any]:
    with queue_lock:
        snapshot = dict(queue_state)

    total = snapshot["total"] or 0
    sent = snapshot["sent"] or 0
    progress = int((sent / total) * 100) if total else 0
    snapshot["progress_percent"] = progress
    snapshot["remaining"] = max(0, total - sent)
    return snapshot


@app.post("/api/queue/pause")
def pause_queue() -> dict[str, Any]:
    with queue_lock:
        if not queue_state["running"]:
            raise HTTPException(status_code=400, detail="Nao ha fila em execucao.")
        queue_state["paused"] = True
    return {"ok": True}


@app.post("/api/queue/resume")
def resume_queue() -> dict[str, Any]:
    with queue_lock:
        if not queue_state["running"]:
            raise HTTPException(status_code=400, detail="Nao ha fila em execucao.")
        queue_state["paused"] = False
    return {"ok": True}


@app.post("/api/queue/cancel")
def cancel_queue() -> dict[str, Any]:
    with queue_lock:
        if not queue_state["running"]:
            raise HTTPException(status_code=400, detail="Nao ha fila em execucao.")
        queue_state["cancelled"] = True
        queue_state["paused"] = False
    return {"ok": True}
