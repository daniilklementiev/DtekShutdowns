import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


URL_DEFAULT = "https://www.dtek-oem.com.ua/ua/shutdowns"
STATE_PATH = Path("state.json")

DISCLAIMER_MARKERS = [
    "Якщо в даний момент у вас відсутнє світло",
    "Просимо перевірити інформацію через 15 хвилин",
]


@dataclass
class OutageInfo:
    address: str
    status_line: Optional[str] = None
    reason: Optional[str] = None
    start_dt: Optional[str] = None
    restore_dt: Optional[str] = None
    restore_raw: Optional[str] = None
    raw_block: Optional[str] = None


def _parse_dt_from_ua(text: str) -> Optional[datetime]:
    text = text.strip()
    try:
        return datetime.strptime(text, "%H:%M %d.%m.%Y")
    except ValueError:
        return None


def parse_outage_from_page_text(page_text: str, address: str) -> OutageInfo:
    lines = [ln.strip() for ln in page_text.splitlines()]
    lines = [ln for ln in lines if ln]

    start_idx = None
    for i, ln in enumerate(lines):
        if "За вашою адресою" in ln or "За цією адресою" in ln:
            start_idx = i
            break
    if start_idx is None:
        for i, ln in enumerate(lines):
            if ln.startswith("Причина:"):
                start_idx = max(0, i - 1)
                break

    block_lines = lines[start_idx:start_idx + 14] if start_idx is not None else []
    raw_block = "\n".join(block_lines) if block_lines else None

    info = OutageInfo(address=address, raw_block=raw_block)

    m = re.search(r"(За (?:вашою|цією) адресою[^\n]*)", page_text)
    if m:
        info.status_line = m.group(1).strip()

    m = re.search(r"Причина:\s*(.+)", page_text)
    if m:
        info.reason = m.group(1).strip()

    m = re.search(r"Час\s+початку\s*[–-]\s*([0-2]\d:[0-5]\d\s+\d{2}\.\d{2}\.\d{4})", page_text)
    if m:
        dt = _parse_dt_from_ua(m.group(1))
        if dt:
            info.start_dt = dt.isoformat(sep=" ")

    m = re.search(r"Орієнтовний\s+час\s+відновлення\s+електроенергії\s*[–-]\s*(.+)", page_text)
    if m:
        info.restore_raw = m.group(1).strip()
        m2 = re.search(r"([0-2]\d:[0-5]\d)\s+(\d{2}\.\d{2}\.\d{4})", info.restore_raw)
        if m2:
            dt2 = _parse_dt_from_ua(f"{m2.group(1)} {m2.group(2)}")
            if dt2:
                info.restore_dt = dt2.isoformat(sep=" ")

    return info


def stable_payload(info: OutageInfo) -> Dict[str, Any]:
    # Сравниваем ТОЛЬКО по времени начала/восстановления (+ restore_raw как fallback).
    return {
        "address": info.address,
        "start_dt": info.start_dt,
        "restore_dt": info.restore_dt,
        "restore_raw": info.restore_raw,
    }


def fingerprint(payload: Dict[str, Any]) -> str:
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def send_telegram(token: str, chat_id: str, text: str) -> int:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(
        url,
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        },
        timeout=25,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return int(data["result"]["message_id"])


def edit_telegram(token: str, chat_id: str, message_id: int, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/editMessageText"
    r = requests.post(
        url,
        data={
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        },
        timeout=25,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")


def format_message(info: OutageInfo) -> str:
    lines = [
        "⚡️ ДТЕК — зміна статусу",
        f"Омєга",
    ]
    if info.status_line:
        lines.append(info.status_line)
    if info.reason:
        lines.append(f"Причина: {info.reason}")
    if info.start_dt:
        lines.append(f"<b>Час початку:</b> {info.start_dt}")
    if info.restore_dt:
        lines.append(f"<b>Орієнтовне відновлення:</b> {info.restore_dt}")
    elif info.restore_raw:
        lines.append(f"<b>Орієнтовне відновлення:</b> {info.restore_raw}")

    # Важно: "Перевірено" НЕ участвует в fingerprint, поэтому не спамит.
    lines.append(f"Перевірено: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
    return "\n".join(lines)


def format_restored_message(address: str, start_dt: Optional[str], restore_dt: Optional[str], restore_raw: Optional[str]) -> str:
    start_part = start_dt or "невідомо"
    end_part = restore_dt or (restore_raw or "невідомо")
    return (
        "✅ Світло з’явилося.\n"
        f"Адреса: {address}\n"
        f"Світла не було: {start_part} — {end_part}"
    )


def is_disclaimer_page(text: str) -> bool:
    t = text.replace("\u00a0", " ")
    return any(m in t for m in DISCLAIMER_MARKERS)


def close_modals_best_effort(page) -> None:
    try:
        btns = page.locator("button.modal__close")
        for i in range(btns.count()):
            try:
                btns.nth(i).click(timeout=600)
            except Exception:
                pass
    except Exception:
        pass


def pick_from_autocomplete(page, input_selector: str, list_selector: str, query: str, pick_fn, timeout_ms: int = 20000):
    page.click(input_selector, timeout=timeout_ms)
    page.fill(input_selector, "")
    page.type(input_selector, query, delay=35)

    page.wait_for_selector(f"{list_selector} > div", timeout=timeout_ms)
    items = page.locator(f"{list_selector} > div")
    count = items.count()
    if count == 0:
        raise RuntimeError(f"Empty autocomplete list for {input_selector} / query={query!r}")

    chosen = None
    for i in range(count):
        t = items.nth(i).inner_text().strip()
        if pick_fn(t):
            chosen = i
            break
    if chosen is None:
        chosen = 0
    items.nth(chosen).click()

def fetch_outage_info(url: str, city: str, street: str, house: str):
    last_err = None
    for attempt in range(1, 4):
        try:
            return _fetch_outage_info_once(url, city, street, house)
        except Exception as e:
            last_err = e
            print(f"[WARN] fetch attempt {attempt}/3 failed: {e}")
    raise last_err

def _fetch_outage_info_once(url: str, city: str, street: str, house: str) -> Tuple[OutageInfo, str]:
    address_str = f"м. {city}, вул. {street}, {house}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
        context = browser.new_context(locale="uk-UA")
        page = context.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_selector(".discon-schedule-table", timeout=60000)
        close_modals_best_effort(page)

        pick_from_autocomplete(
            page, "#city", "#cityautocomplete-list", city,
            pick_fn=lambda t: t.strip().lower() == city.strip().lower(),
        )
        pick_from_autocomplete(
            page, "#street", "#streetautocomplete-list", street,
            pick_fn=lambda t: street.lower() in t.lower(),
        )
        page.wait_for_function(
            "() => { const el = document.querySelector('#house_num'); return el && !el.disabled; }",
            timeout=30000
        )
        pick_from_autocomplete(
            page, "#house_num", "#house_numautocomplete-list", house,
            pick_fn=lambda t: t.strip() == house.strip(),
        )

        try:
            page.wait_for_selector("text=За вашою адресою", timeout=15000)
        except PlaywrightTimeoutError:
            page.wait_for_timeout(1200)

        body_text = page.inner_text("body")
        browser.close()

    lowered = body_text.lower()
    antibot = ["incapsula", "access denied", "forbidden", "captcha", "request unsuccessful"]
    if any(x in lowered for x in antibot):
        raise RuntimeError("Antibot/captcha page returned by DTEK site.")

    return parse_outage_from_page_text(body_text, address_str), body_text


def load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")

    city = os.getenv("CITY", "Одеса")
    street = os.getenv("STREET", "Краснова")
    house = os.getenv("HOUSE", "1")
    url = os.getenv("DTEK_URL", URL_DEFAULT)

    if not tg_token or not tg_chat_id:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID secrets")

    prev = load_state()
    prev_fp = prev.get("fingerprint")
    last_message_id = prev.get("message_id")

    info, body_text = fetch_outage_info(url, city, street, house)

    # Определяем "нет отключения сейчас" (дисклеймер) или вообще нет времени
    no_outage_now = is_disclaimer_page(body_text) or (not info.start_dt and not info.restore_dt and not info.restore_raw)

    # Если свет появился (раньше было отключение, а сейчас нет) — шлём отдельное уведомление
    if no_outage_now:
        last_start = prev.get("last_outage_start")
        last_restore = prev.get("last_outage_restore")
        last_restore_raw = prev.get("last_outage_restore_raw")

        had_outage_before = bool(last_start or last_restore or last_restore_raw)

        if had_outage_before:
            restored_msg = format_restored_message(info.address, last_start, last_restore, last_restore_raw)
            send_telegram(tg_token, tg_chat_id, restored_msg)
            print("[TG] restored sent")

            # очищаем outage-данные, чтобы не спамить на каждом запуске
            save_state({
                "fingerprint": "NO_OUTAGE",
                "payload": {"address": info.address, "status": "NO_OUTAGE"},
                "message_id": last_message_id,  # оставим, чтобы следующее outage могло редактировать старое сообщение
                "updated_at": datetime.utcnow().isoformat(),
                "last_outage_start": None,
                "last_outage_restore": None,
                "last_outage_restore_raw": None,
            })
            print("[STATE] cleared outage")
        else:
            print("[OK] no outage (nothing to restore)")
        return

    # Есть отключение — считаем fingerprint по временам
    payload = stable_payload(info)
    fp = fingerprint(payload)

    print("=" * 80)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"fingerprint={fp}")
    print("=" * 80)

    if prev_fp is None:
        save_state({
            "fingerprint": fp,
            "payload": payload,
            "message_id": last_message_id,
            "updated_at": datetime.utcnow().isoformat(),
            "last_outage_start": info.start_dt,
            "last_outage_restore": info.restore_dt,
            "last_outage_restore_raw": info.restore_raw,
        })
        print("[INIT] baseline saved")
        return

    if fp == prev_fp:
        print("[OK] no changes")
        return

    # Изменилось => редактируем прошлое сообщение (если было), иначе шлём новое
    msg = format_message(info)

    if last_message_id:
        try:
            edit_telegram(tg_token, tg_chat_id, int(last_message_id), msg)
            print("[TG] edited")
            message_id = int(last_message_id)
        except Exception as e:
            print(f"[WARN] edit failed, sending new: {e}")
            message_id = send_telegram(tg_token, tg_chat_id, msg)
            print("[TG] sent (new)")
    else:
        message_id = send_telegram(tg_token, tg_chat_id, msg)
        print("[TG] sent (new)")    

    save_state({
        "fingerprint": fp,
        "payload": payload,
        "message_id": message_id,
        "updated_at": datetime.utcnow().isoformat(),
        "last_outage_start": info.start_dt,
        "last_outage_restore": info.restore_dt,
        "last_outage_restore_raw": info.restore_raw,
    })
    print("[STATE] updated")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        msg = str(e).lower()
        transient = any(x in msg for x in [
            "antibot", "captcha", "access denied", "forbidden",
            "timeout", "timed out", "net::", "navigation",
            "request unsuccessful"
        ])
        print(f"[ERROR] {type(e).__name__}: {e}")

        # Для временных/сетевых проблем не валим job
        if transient:
            raise SystemExit(0)

        # Для прочих ошибок пусть падает (чтобы ты видел)
        raise