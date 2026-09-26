"""MCP-сервер «Конвертер» — курсы валют и конвертация (ExchangeRate-API).

Отдельный MCP-сервер проекта: предоставляет инструменты для работы с курсами
валют через API https://app.exchangerate-api.com (v6):
  * list_currencies       — список поддерживаемых кодов валют;
  * get_rates             — курсы базовой валюты ко всем остальным;
  * convert_currency      — перевести сумму из одной валюты в другую;
  * get_rate              — курс между двумя конкретными валютами.

ДОПОЛНИТЕЛЬНО (см. docs/task4.md) — РАБОТА ПО РАСПИСАНИЮ с накоплением
данных в JSON и агрегацией:
  * schedule_rate         — поставить периодический сбор курса пары в JSON;
  * list_jobs             — список заданий расписания;
  * cancel_job            — отменить задание по id;
  * run_due               — исполнить созревшие задания (ленивая и явная);
  * collect_now           — внеплановое разовое сохранение курса;
  * rate_points           — накопленные точки курса пары;
  * rate_summary          — агрегат (мин/макс/среднее/изменение) по парам.

API-ключ берётся из файла exch.txt (одна строка — ключ). Файл НЕ коммитится.
Задания/точки хранятся в общем JSON заданий (session/mcp_jobs.json), см.
rtk_app/jobs_store.py.

Запуск (как stdio-подпроцесс MCP):
    python currency_server.py

Переключение проекта на этот сервер — в rtk_app/config.py (MCP_SERVERS).
"""

import json
import os
import urllib.error
import urllib.request

from mcp.server.mcpserver import MCPServer

# BASE_DIR — папка проекта (рядом с config.py); учитываем и запуск из др. CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Хранилище заданий расписания — общий модуль проекта (rtk_app/jobs_store.py).
# MCP-сервер запускается из корня проекта, поэтому импорт доступен; на случай
# нестандартного CWD добавляем папку проекта в sys.path.
import sys
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
from rtk_app.jobs_store import JobsStore, now_str

mcp = MCPServer("currency-converter")

# Базовый адрес API v6 (Standard / Pair Conversion).
API_BASE = "https://v6.exchangerate-api.com/v6"

# Стандартный набор валют, который возвращает get_rates без фильтра.
# (Полный список поддерживаемых кодов — десятки валют; фильтр оставлен
# необязательным, по умолчанию отдаём самые ходовые.)
COMMON_CURRENCIES = (
    "USD", "EUR", "GBP", "CNY", "JPY", "CHF", "RUB", "KZT", "BYN",
    "UAH", "TRY", "AED", "AMD", "GEL", "KGS", "UZS", "AZN", "INR",
    "CAD", "AUD", "KRW", "PLN", "CZK", "TRY",
)


# --------------------------------------------------------------------------
# Ключ и HTTP
# --------------------------------------------------------------------------
def _api_key():
    """Читает API-ключ ExchangeRate-API из exch.txt (одна строка)."""
    path = os.environ.get("EXCH_KEY_FILE") or os.path.join(BASE_DIR, "exch.txt")
    if not os.path.isfile(path):
        raise RuntimeError("не найден файл с API-ключом: %s" % path)
    with open(path, encoding="utf-8") as f:
        lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
    if not lines:
        raise RuntimeError("exch.txt пуст — положите в него API-ключ одной строкой")
    return lines[0]


def _get_json(url):
    """GET-запрос к API и разбор JSON-ответа.

    Возвращает уже разобранный dict. Сетевые/HTTP-ошибки и ошибки API
    приводятся к понятному текстовому сообщению через исключение.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "rtk-mcp-currency/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError("HTTP-%s от ExchangeRate-API" % exc.code)
    except urllib.error.URLError as exc:
        raise RuntimeError("нет связи с ExchangeRate-API: %s" % exc.reason)
    if not isinstance(data, dict):
        raise RuntimeError("неожиданный ответ ExchangeRate-API")
    if data.get("result") == "error":
        raise RuntimeError(_error_text(data.get("error-type")))
    return data


def _error_text(err_type):
    """Переводит код ошибки API на русский."""
    mapping = {
        "unsupported-code": "неподдерживаемый код валюты (см. list_currencies)",
        "malformed-request": "некорректный запрос к ExchangeRate-API",
        "invalid-key": "недействительный API-ключ (проверьте exch.txt)",
        "inactive-account": "аккаунт ExchangeRate-API не активирован",
        "quota-reached": "исчерпана квота запросов ExchangeRate-API",
        "unknown-code": "неизвестный код валюты",
    }
    return mapping.get(str(err_type), "ошибка ExchangeRate-API: %s" % err_type)


def _norm_code(code):
    """Нормализует код валюты: верхний регистр, без пробелов."""
    return str(code or "").strip().upper()


def _series_key(frm, to):
    """Имя серии накопленных точек для пары валют: "USD>RUB"."""
    return "%s>%s" % (_norm_code(frm), _norm_code(to))


# --------------------------------------------------------------------------
# Работа по расписанию (задания + накопление точек + агрегация)
# --------------------------------------------------------------------------
_JOBS = JobsStore(server="currency")


def _jobs():
    """Хранилище заданий этого сервера (общий JSON, пространство "currency")."""
    return _JOBS


def _fetch_rate(frm, to):
    """Тянет текущий курс пары у API. Возвращает (rate, updated)."""
    data = _get_json("%s/%s/pair/%s/%s" % (API_BASE, _api_key(), frm, to))
    return data.get("conversion_rate"), (data.get("time_last_update_utc") or "")


def _record_rate(frm, to, source=""):
    """Сохраняет текущий курс пары как точку в JSON. Возвращает (rate, count).

    Точка: {"t": "YYYY-MM-DD HH:MM:SS", "rate": число, "src": метка источника}.
    """
    frm = _norm_code(frm)
    to = _norm_code(to)
    rate, _updated = _fetch_rate(frm, to)
    if rate is None:
        raise RuntimeError("API не вернул курс для %s>%s" % (frm, to))
    count = _jobs().add_point(_series_key(frm, to),
                              {"t": now_str(), "rate": rate, "src": str(source)})
    return rate, count


def _run_due():
    """Исполняет СОЗРЕВШИЕ задания сбора курсов. Возвращает список строк-отчётов.

    Вызывается ЛЕНИВО из других инструментов (чтобы данные копились, когда
    агент обращается к серверу) и ЯВНО из run_due().
    """
    reports = []
    for job in _jobs().due_jobs():
        if job.get("kind") != "rate":
            continue
        params = job.get("params") or {}
        frm = params.get("from_currency") or params.get("base") or ""
        to = params.get("to_currency") or params.get("target") or ""
        try:
            rate, count = _record_rate(frm, to, source=job.get("id", "job"))
            _jobs().mark_run(job["id"], "курс %s" % rate)
            reports.append("Задание %s: %s>%s = %s (точек: %d)"
                           % (job["id"], _norm_code(frm), _norm_code(to),
                              rate, count))
        except Exception as exc:
            # Задание помечаем исполненным и на разовом — «done», периодич.
            # перепланируем; ошибка не должна «залипать» навсегда.
            _jobs().mark_run(job["id"], "ошибка: %s" % exc)
            reports.append("Задание %s: ошибка — %s" % (job["id"], exc))
    return reports


def _fmt_jobs(jobs):
    """Читаемое представление списка заданий."""
    if not jobs:
        return "Заданий нет."
    lines = []
    for j in jobs:
        params = j.get("params") or {}
        if j.get("kind") == "rate":
            desc = "%s>%s" % (params.get("from_currency", ""),
                              params.get("to_currency", ""))
        else:
            desc = str(params)
        every = int(j.get("every_minutes", 0) or 0)
        period = ("каждые %d мин" % every) if every > 0 else "разовое"
        status = "выполнено" if j.get("done") else \
            ("след: %s" % j.get("next_run", ""))
        lines.append("  %s [%s] %s — %s; %s; запусков: %d"
                     % (j.get("id"), j.get("kind"), desc, period, status,
                        int(j.get("runs", 0) or 0)))
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Инструменты MCP
# --------------------------------------------------------------------------
@mcp.tool()
def list_currencies() -> str:
    """Список поддерживаемых кодов валют.

    Возвращает распространённые коды (ISO 4217). Полный список — в
    документации ExchangeRate-API; здесь показаны основные.
    """
    codes = []
    seen = set()
    for c in COMMON_CURRENCIES:
        if c not in seen:
            seen.add(c)
            codes.append(c)
    return "Поддерживаемые коды валют (ISO 4217):\n" + ", ".join(codes)


@mcp.tool()
def get_rate(from_currency: str, to_currency: str) -> str:
    """Курс обмена между двумя конкретными валютами.

    from_currency — код исходной валюты (например, "USD");
    to_currency   — код целевой валюты (например, "RUB").
    Возвращает курс: сколько единиц to_currency в 1 единице from_currency.
    """
    # ЛЕНИВЫЙ ПРОГОН: при любом обращении к серверу сначала отрабатываем
    # созревшие задания расписания, чтобы накопление шло без демона.
    _run_due()
    frm = _norm_code(from_currency)
    to = _norm_code(to_currency)
    if not frm or not to:
        return "Укажите коды валют from_currency и to_currency (например, USD и RUB)."
    try:
        data = _get_json("%s/%s/pair/%s/%s" % (API_BASE, _api_key(), frm, to))
    except Exception as exc:
        return "Ошибка получения курса: %s" % exc
    rate = data.get("conversion_rate")
    updated = data.get("time_last_update_utc") or ""
    return ("1 %s = %s %s (обновлено: %s)"
            % (frm, rate, to, updated))


@mcp.tool()
def convert_currency(amount: float, from_currency: str, to_currency: str) -> str:
    """Перевести сумму из одной валюты в другую по текущему курсу.

    amount        — сумма (число);
    from_currency — код исходной валюты (например, "USD");
    to_currency   — код целевой валюты (например, "RUB").
    """
    frm = _norm_code(from_currency)
    to = _norm_code(to_currency)
    if not frm or not to:
        return "Укажите коды валют from_currency и to_currency (например, USD и RUB)."
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return "Укажите числовую сумму amount."
    try:
        data = _get_json("%s/%s/pair/%s/%s/%.4f"
                         % (API_BASE, _api_key(), frm, to, amount))
    except Exception as exc:
        return "Ошибка конвертации: %s" % exc
    rate = data.get("conversion_rate")
    result = data.get("conversion_result")
    updated = data.get("time_last_update_utc") or ""
    return ("%.4g %s = %.4g %s (курс: 1 %s = %s %s; обновлено: %s)"
            % (amount, frm, result, to, frm, rate, to, updated))


@mcp.tool()
def get_rates(base_currency: str, currencies: str = "") -> str:
    """Курсы базовой валюты к другим валютам.

    base_currency — код базовой валюты (например, "USD");
    currencies    — необязательный список кодов через запятую для фильтра
                    (например, "EUR,RUB,CNY"). Если пусто — вернётся набор
                    ходовых валют (см. list_currencies).
    """
    base = _norm_code(base_currency)
    if not base:
        return "Укажите код базовой валюты base_currency (например, USD)."
    try:
        data = _get_json("%s/%s/latest/%s" % (API_BASE, _api_key(), base))
    except Exception as exc:
        return "Ошибка получения курсов: %s" % exc
    rates = data.get("conversion_rates") or {}
    if not isinstance(rates, dict) or not rates:
        return "ExchangeRate-API не вернул курсы."

    if currencies.strip():
        wanted = [_norm_code(c) for c in currencies.split(",") if c.strip()]
    else:
        wanted = list(COMMON_CURRENCIES)

    lines = []
    missing = []
    for code in wanted:
        if code == base:
            continue
        if code in rates:
            lines.append("  1 %s = %s %s" % (base, rates[code], code))
        else:
            missing.append(code)
    updated = data.get("time_last_update_utc") or ""
    out = ["Курсы %s (обновлено: %s):" % (base, updated)]
    out.append("\n".join(lines) if lines else "  нет данных по запрошенным валютам")
    if missing:
        out.append("  (не поддержаны: %s)" % ", ".join(missing))
    return "\n".join(out)


# --------------------------------------------------------------------------
# Инструменты расписания (сбор курсов по расписанию + агрегат)
# --------------------------------------------------------------------------
@mcp.tool()
def schedule_rate(from_currency: str, to_currency: str,
                  every_minutes: int = 0, at: str = "",
                  in_minutes: int = -1, first_now: bool = False) -> str:
    """Поставить ПЕРИОДИЧЕСКИЙ (или отложенный) сбор курса пары в JSON.

    from_currency, to_currency — пара валют (например, USD и RUB);
    every_minutes — периодичность в минутах (например, 60 или 1440);
                    если 0 — задание разовое (см. at/in_minutes/first_now);
    at            — время первого запуска "YYYY-MM-DD HH:MM" (для разового/первого);
    in_minutes    — первый запуск через N минут от текущего момента;
    first_now     — True: первое сохранение сделать СРАЗУ (удобно для периодики).

    Каждое созревание записывает точку (время, курс) в JSON-серию "FROM>TO".
    Задания исполняются ЛЕНИВО (при обращении к серверу) и явным run_due().
    Для периодического сбора удобно: every_minutes=1440, first_now=True.
    """
    frm = _norm_code(from_currency)
    to = _norm_code(to_currency)
    if not frm or not to:
        return "Укажите коды валют from_currency и to_currency (например, USD и RUB)."
    every = max(0, int(every_minutes or 0))
    job = _jobs().add_job(
        "rate", {"from_currency": frm, "to_currency": to},
        every_minutes=every,
        at=(at or None),
        in_minutes=(None if in_minutes is None or int(in_minutes) < 0
                    else int(in_minutes)),
    )
    # Если просили «сразу» — сделаем первое сохранение немедленно.
    extra = ""
    if first_now:
        try:
            rate, count = _record_rate(frm, to, source=job["id"])
            _jobs().mark_run(job["id"], "курс %s" % rate)
            extra = " Первое сохранение: %s (%s>%s), точек: %d." % (
                rate, frm, to, count)
        except Exception as exc:
            extra = " Первое сохранение не удалось: %s" % exc
    period = ("каждые %d мин" % every) if every > 0 else "разовое"
    return ("Задание %s создано: сбор курса %s>%s (%s), след. запуск: %s.%s"
            % (job["id"], frm, to, period, job.get("next_run", ""), extra))


@mcp.tool()
def list_jobs() -> str:
    """Список заданий расписания этого сервера (сбор курсов)."""
    return "Задания расписания (Конвертер):\n" + _fmt_jobs(_jobs().list_jobs())


@mcp.tool()
def cancel_job(job_id: str) -> str:
    """Отменить задание расписания по его id."""
    if _jobs().delete_job(str(job_id or "").strip()):
        return "Задание %s отменено." % job_id
    return "Задание %s не найдено." % job_id


@mcp.tool()
def run_due() -> str:
    """Исполнить все СОЗРЕВШИЕ задания сбора курсов ПРЯМО СЕЙЧАС.

    Полезно как «тик» планировщика: даже без демона можно периодически
    обращаться к этому инструменту (или просто вызывать другие — они тоже
    прогоняют созревшие задания).
    """
    reports = _run_due()
    if not reports:
        return "Созревших заданий нет."
    return "Исполнено заданий: %d\n%s" % (len(reports), "\n".join(reports))


@mcp.tool()
def collect_now(from_currency: str, to_currency: str) -> str:
    """Внеплановое разовое сохранение текущего курса пары в JSON.

    Удобно, чтобы сделать точку вне расписания.
    """
    frm = _norm_code(from_currency)
    to = _norm_code(to_currency)
    if not frm or not to:
        return "Укажите коды валют from_currency и to_currency (например, USD и RUB)."
    try:
        rate, count = _record_rate(frm, to, source="manual")
    except Exception as exc:
        return "Не удалось сохранить курс: %s" % exc
    return ("Курс %s>%s = %s сохранён (точек в серии: %d)." % (frm, to, rate, count))


@mcp.tool()
def rate_points(from_currency: str, to_currency: str, limit: int = 50) -> str:
    """Накопленные точки курса пары (время, курс).

    from_currency, to_currency — пара валют;
    limit — сколько ПОСЛЕДНИХ точек показать (0 — все).
    """
    frm = _norm_code(from_currency)
    to = _norm_code(to_currency)
    series = _series_key(frm, to)
    pts = _jobs().get_series(series)
    if not pts:
        return "По паре %s>%s ещё нет накопленных точек." % (frm, to)
    lim = max(0, int(limit or 0))
    shown = pts[-lim:] if lim > 0 else pts
    lines = ["%s>%s — точек всего %d (показано %d):"
             % (frm, to, len(pts), len(shown))]
    for p in shown:
        lines.append("  %s: %s" % (p.get("t", ""), p.get("rate", "")))
    return "\n".join(lines)


@mcp.tool()
def rate_summary(from_currency: str = "", to_currency: str = "",
                 period: str = "all") -> str:
    """АГРЕГИРОВАННЫЙ результат по накопленным курсам (сводка).

    from_currency, to_currency — пара валют; если не заданы — сводка по ВСЕМ
    накопленным парам.
    period — за какой период: "day" (сутки), "week", "month" или "all" (всё).

    Возвращает по каждой паре: число точек, первую/последнюю точку, минимум,
    максимум, средний курс и ИЗМЕНЕНИЕ (последняя − первая, в абс. и %).
    """
    frm = _norm_code(from_currency) if from_currency else ""
    to = _norm_code(to_currency) if to_currency else ""
    if frm and to:
        names = [_series_key(frm, to)]
    elif not frm and not to:
        names = _jobs().series_names()
    else:
        return "Укажите ОБЕ валюты (from_currency и to_currency) или ни одной."

    import time as _t
    # Граница периода — сколько секунд назад считать точки «свежими».
    secs = {"day": 86400, "week": 604800, "month": 2592000}.get(
        str(period or "all").strip().lower())
    now_ts = _t.time()

    blocks = []
    for series in names:
        pts = _jobs().get_series(series)
        # Фильтр по периоду (по времени точки).
        if secs is not None:
            kept = []
            for p in pts:
                try:
                    ts = _t.mktime(_t.strptime(str(p.get("t", "")),
                                               "%Y-%m-%d %H:%M:%S"))
                except Exception:
                    continue
                if now_ts - ts <= secs:
                    kept.append(p)
            pts = kept
        vals = []
        for p in pts:
            try:
                vals.append((str(p.get("t", "")), float(p.get("rate"))))
            except (TypeError, ValueError):
                continue
        if not vals:
            continue
        rates = [v for _t0, v in vals]
        first_t, first_v = vals[0]
        last_t, last_v = vals[-1]
        mn, mx = min(rates), max(rates)
        avg = sum(rates) / len(rates)
        diff = last_v - first_v
        pct = (diff / first_v * 100.0) if first_v else 0.0
        blocks.append(
            "  %s — точек: %d (за период: %s)\n"
            "    первая: %s = %s; последняя: %s = %s\n"
            "    мин: %s; макс: %s; среднее: %.4f\n"
            "    изменение: %+.4f (%+.2f%%)"
            % (series, len(vals), period, first_t, first_v, last_t, last_v,
               mn, mx, avg, diff, pct))

    if not blocks:
        return ("Нет накопленных данных%s. Поставьте задание через "
                "schedule_rate или вызовите collect_now."
                % (" по паре %s>%s" % (frm, to) if (frm and to) else ""))
    return "СВОДКА по накопленным курсам (период: %s):\n%s" % (
        period, "\n".join(blocks))


if __name__ == "__main__":
    # Транспорт по умолчанию — stdio (как подпроцесс MCP-клиента проекта).
    mcp.run()
