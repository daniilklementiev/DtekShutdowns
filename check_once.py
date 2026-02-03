import hashlib
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


URL_DEFAULT = "https://www.dtek-oem.com.ua/ua/shutdowns"
STATE_PATH = Path("state.json")


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
    d = asdict(info)
    # Если хочешь сравнивать только ключевые поля — раскомментируй:
    # d = {k: d[k] for k in ["address", "status_line", "reason", "start_dt", "restore_raw", "restore_dt"]}
    return d


def fingerprint(payload: Dict[str, Any]) -> str:
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def send_telegram(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(
        url,
        data={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        timeout=25,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")


def format_message(info: OutageInfo) -> str:
    lines = [
        "⚡️ ДТЕК — зміна статусу",
        f"Адреса: {info.address}",
    ]
    if info.status_line:
        lines.append(info.status_line)
    if info.reason:
        lines.append(f"Причина: {info.reason}")
    if info.start_dt:
        lines.append(f"Час початку: {info.start_dt}")
    if info.restore_dt:
        lines.append(f"Орієнтовне відновлення: {info.restore_dt}")
    elif info.restore_raw:
        lines.append(f"Орієнтовне відновлення: {info.restore_raw}")

    if not (info.status_line or info.reason or info.start_dt or info.restore_raw) and info.raw_block:
        lines.append("")
        lines.append("RAW:")
        lines.append(info.raw_block)

    lines.append(f"Перевірено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
    return "\n".join(lines)


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


def fetch_outage_info(url: str, city: str, street: str, house: str) -> OutageInfo:
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
        page.wait_for_function("() => { const el = document.querySelector('#house_num'); return el && !el.disabled; }", timeout=30000)
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

    # антибот-страницы лучше не считать "новым состоянием"
    lowered = body_text.lower()
    antibot = ["incapsula", "access denied", "forbidden", "captcha", "request unsuccessful"]
    if any(x in lowered for x in antibot):
        raise RuntimeError("Antibot/captcha page returned by DTEK site.")

    return parse_outage_from_page_text(body_text, address_str)


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

    info = fetch_outage_info(url, city, street, house)
    payload = stable_payload(info)
    fp = fingerprint(payload)

    # вывод в лог Actions
    print("=" * 80)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"fingerprint={fp}")
    print("=" * 80)

    if prev_fp is None:
        # первый запуск: просто сохранить baseline, чтобы не спамить
        save_state({"fingerprint": fp, "payload": payload, "updated_at": datetime.utcnow().isoformat()})
        print("[INIT] baseline saved")
        return

    if fp == prev_fp:
        print("[OK] no changes")
        return

    # изменилось => TG + обновить state
    msg = format_message(info)
    send_telegram(tg_token, tg_chat_id, msg)
    print("[TG] sent")

    save_state({"fingerprint": fp, "payload": payload, "updated_at": datetime.utcnow().isoformat()})
    print("[STATE] updated")


if __name__ == "__main__":
    main()