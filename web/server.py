

"""Встроенный HTTP-сервер (стандартная библиотека).

Чат "только в окне": сервер ничего не хранит на диске. История ведётся в
памяти вкладки и присылается целиком в POST /api/ask.

Сервер работает через АГЕНТА (rtk_app.agent.Agent) — отдельную сущность,
которая инкапсулирует всю логику запросов к LLM. Агент принимает вопрос и
историю диалога, сам обращается к моделям через API и возвращает готовый
результат (html, text, ответы моделей). HTTP-обработчик лишь передаёт данные
между браузером и агентом.

Запрос пользователя уходит агенту, который опрашивает модели:
  - DeepSeek-flash,
  - GigaChat (базовая, простая).

JSON-API:
    GET  /                  - страница (index.html)
    GET  /css/*,/js/*       - стили и скрипты
    GET  /api/model         - список моделей, которые обслуживает агент
    GET  /api/session       - сохранённая история + стратегия + facts + ветки
    POST /api/ask           - {question, models[], max_tokens?, compact?,
                               strategy?} -> ответ
    POST /api/newchat       - начать новый разговор (очистить историю)
    POST /api/compact       - {enabled, keep} -> настройки сжатия (summary)
    POST /api/compact_summary - {keep} -> дописать вытесненное в summary
    POST /api/strategy      - {strategy, window} -> стратегия контекста
                              (none / sliding / facts / branch)
    POST /api/facts         - {facts:{ключ:значение}} -> сохранить блок facts
    POST /api/memory        - {type:"working"|"longterm", …} -> память агента
                              (действия: set/delete/replace/clear, GET-снимок)
    POST /api/branches      - {action:"create"|"switch"|"delete"|"rename", …}
                              -> ветки (rename: {index, name})
    POST /api/task          - состояние задачи (Task State Machine):
                              {action:"start"|"advance"|"pause"|"resume"|
                               "finish"|"reset"|"state", …} -> этап/шаг/
                              ожидаемое действие (planning->execution->
                              validation->done), пауза/продолжение.
    GET  /api/invariants    - инварианты (правила-ограничения) + категории
    POST /api/invariants    - {action:"add"|"update"|"delete"|"replace"|
                               "clear"|"state", …} -> инварианты (отдельно от
                              диалога; жёсткие правила, нарушать нельзя)
    GET  /api/profiles      - список профилей (персон) + активный
    POST /api/profiles      - {action:"create"|"switch"|"update"|"delete", …}
                              -> профили (персоны): при создании задаются
                              name, model (модель персоны), character
                              (характер/тон) и style (характер ответов).
                              Ответы даёт АКТИВНАЯ персона своей моделью;
                              при отсутствии персон диалог идёт с моделями.
    GET  /api/mcp           - состояние MCP: {enabled, model, tools, status}
                              (настройки + последний статус сервера)
    POST /api/mcp           - {action, …} -> настройки и статус MCP:
                              "state"  — вернуть текущее состояние (по умолч.);
                              "set"    — сохранить {enabled?, model?};
                              "status" — ПРОВЕРИТЬ статус MCP-сервера
                                         (подключиться, получить список
                                         инструментов и вернуть результат).
    """
import json
import mimetypes
import os
import webbrowser
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from rtk_app import config
from rtk_app.agent import Agent
from rtk_app.session_store import SessionStore

try:
    # MCP-клиент (опциональная зависимость): сервер проекта должен
    # запускаться даже без пакета mcp — тогда статус MCP будет «недоступен».
    from rtk_app import mcp_client
except Exception:  # pragma: no cover — зависит от окружения
    mcp_client = None


class _ServerState:
    """Глобальное состояние сервера: агент (единая сущность) и сессия."""
    agent = None
    session = None
    # Настройки MCP (включается чекбоксом в левой колонке):
    #   enabled — использовать ли инструменты MCP;
        #   model   — метка модели, применяемой при работе с MCP;
    #   status  — последний результат проверки статуса MCP-сервера.
    mcp_enabled = getattr(config, "MCP_ENABLED", False)
    mcp_model = getattr(config, "MCP_MODEL", "")
    # id выбранного MCP-сервера (см. config.MCP_SERVERS).
    mcp_server = getattr(config, "MCP_SERVER_DEFAULT", "calendar")
    mcp_status = None
    mcp_lock = threading.Lock()


def _mcp_settings_file():
    """Путь к файлу настроек MCP (в папке сессии)."""
    return getattr(config, "MCP_SETTINGS_FILE", None)


def _load_mcp_settings():
    """Загружает настройки MCP из файла (если есть), заполняя состояние."""
    path = _mcp_settings_file()
    if not path or not os.path.isfile(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _ServerState.mcp_enabled = bool(data.get("enabled",
                                                   _ServerState.mcp_enabled))
            model = data.get("model")
            if isinstance(model, str):
                _ServerState.mcp_model = model
            server = data.get("server")
            if isinstance(server, str) and server:
                _ServerState.mcp_server = server
    except Exception as exc:
        print("[MCP] не удалось загрузить настройки: %s" % exc, flush=True)


def _save_mcp_settings():
    """Сохраняет настройки MCP на диск (в папку сессии)."""
    path = _mcp_settings_file()
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"enabled": _ServerState.mcp_enabled,
                       "model": _ServerState.mcp_model,
                       "server": _ServerState.mcp_server}, f,
                      ensure_ascii=False, indent=2)
    except Exception as exc:
        print("[MCP] не удалось сохранить настройки: %s" % exc, flush=True)

def _mcp_state_payload(check=False):
    """Собирает состояние MCP для ответа клиенту.

    check=True — предварительно ПРОВЕРЯЕТ статус сервера (подключение +
    список инструментов) и обновляет сохранённый статус.
        """
    if check:
        _ServerState.mcp_status = _probe_mcp_status()
    status = _ServerState.mcp_status
    servers = []
    if mcp_client is not None:
        try:
            servers = mcp_client.mcp_servers()
        except Exception:
            servers = []
    return {
        "ok": True,
        "enabled": bool(_ServerState.mcp_enabled),
        "model": _ServerState.mcp_model or "",
        "server": _ServerState.mcp_server or "",
        "servers": servers,
        "available": mcp_client is not None,
        "status": status,
    }


def _probe_mcp_status():
    """Проверяет статус MCP-сервера (через mcp_client.mcp_status)."""
    if mcp_client is None:
        return {"ok": False, "connected": False, "tools_count": 0,
                "tools": [], "server": "",
                "error": "MCP-клиент недоступен (не установлен пакет mcp)."}
    try:
        return mcp_client.mcp_status(server_id=(_ServerState.mcp_server or None))
    except Exception as exc:
        return {"ok": False, "connected": False, "tools_count": 0,
                "tools": [], "server": "", "error": str(exc)}


def _collect_due_reminders():
    """Собирает НАСТУПИВШИЕ напоминания календаря (для доставки в чат).

    Напоминания выдаёт MCP-инструмент ``run_due`` календарного сервера —
    он же делает «ленивый прогон» и возвращает готовые тексты. Вызываем его
    АВТОМАТИЧЕСКИ при каждом запросе пользователя, чтобы напоминание
    приходило САМО, а не только когда модель «догадается» вызвать инструмент.

    Возвращает список строк-напоминаний (может быть пустым). Ошибки MCP
    игнорируются (возвращается []): доставка напоминаний не должна ломать
    основной ответ. Работает только для календарного сервера.
    """
    if mcp_client is None:
        return []
    # Только у календарного MCP-сервера есть инструмент run_due.
    server_id = _ServerState.mcp_server or None
    try:
        res = mcp_client.mcp_call_tool("run_due", {}, server_id=server_id)
    except Exception as exc:
        print("[REMINDER] не удалось собрать напоминания: %s" % exc,
              flush=True)
        return []
    if not res.get("ok"):
        # Инструмента run_due нет (другой сервер) или ошибка — молча пропускаем.
        return []
    text = (res.get("text") or "").strip()
    if not text or "нет" in text.lower() and "напоминан" in text.lower():
        # «Наступивших напоминаний нет.» — пустой результат.
        return []
    # Ответ инструмента run_due: первая строка — «Напоминаний: N», далее тексты
    # напоминаний. Отделяем служебную шапку, если она есть.
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if lines and lines[0].lower().startswith("напоминаний:"):
        lines = lines[1:]
    return lines


def _set_agent(agent):
    """Запоминает агента, который обслуживает все запросы."""
    _ServerState.agent = agent


def _ensure_session():
    """Лениво создаёт единственное хранилище сессии диалога."""
    if _ServerState.session is None:
        _ServerState.session = SessionStore()
    return _ServerState.session


class WebRequestHandler(BaseHTTPRequestHandler):
    server_version = "MultiModelChat/1.0"

    @property
    def agent(self):
        """Единый агент (rtk_app.agent.Agent), созданный при старте сервера."""
        return _ServerState.agent

    @property
    def session(self):
        """Хранилище сессии диалога (rtk_app.session_store.SessionStore)."""
        return _ensure_session()

    def _send_bytes(self, status, body, content_type):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status, obj):
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._send_bytes(status, payload, "application/json; charset=utf-8")

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length <= 0:
            return None
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def log_message(self, fmt, *args):
        print("[%s] %s" % (self.log_date_time_string(), fmt % args))

    # ---------------- GET ----------------
    def do_GET(self):
        import urllib.parse
        path = urllib.parse.urlparse(self.path).path.rstrip("/") or "/"
        if path in ("/", "/index.html"):
            return self._serve_static_safe("index.html")
        if path.startswith(("/css/", "/js/")):
            return self._serve_static_safe(path.lstrip("/"))
        if path == "/api/model":
            return self._send_json(200, {
                "ok": True,
                "model": self.agent.label if self.agent else config.MODEL,
                "models": [m["label"] for m in
                           (self.agent.available() if self.agent else [])],
                "available": (self.agent.available() if self.agent else []),
            })
        if path == "/api/session":
            has = self.session.has_history()
            return self._send_json(200, {
                "ok": True,
                "has_history": has,
                "messages": self.session.snapshot(),
                "compact": self.session.get_compact(),
                "context": self.session.context_stats(),
                "strategy": self.session.get_strategy(),
                "facts": self.session.get_facts(),
                "branches": self.session.branches_state(),
                "memory": self.session.memory_state(),
                "task": self.session.get_task_state(),
                "invariants": self.session.invariants_state(),
                "profiles": self.session.profiles_state(),
            })
        if path == "/api/profiles":
            state = self.session.profiles_state()
            return self._send_json(200, {
                "ok": True,
                "profiles": state["profiles"],
                "active": state["active"],
            })
        if path == "/api/invariants":
            return self._send_json(200, self.session.invariants_state())
        if path == "/api/mcp":
            return self._send_json(200, _mcp_state_payload(check=False))
        self._send_json(404, {"ok": False, "error": "Not Found"})

    def do_POST(self):
        import urllib.parse
        if urllib.parse.urlparse(self.path).path == "/api/ask":
            return self._handle_ask()
        if urllib.parse.urlparse(self.path).path == "/api/newchat":
            self.session.reset()
            return self._send_json(200, {"ok": True,
                                         "messages": self.session.snapshot()})
        if urllib.parse.urlparse(self.path).path == "/api/compact":
            return self._handle_compact()
        if urllib.parse.urlparse(self.path).path == "/api/compact_summary":
            return self._handle_compact_summary()
        if urllib.parse.urlparse(self.path).path == "/api/strategy":
            return self._handle_strategy()
        if urllib.parse.urlparse(self.path).path == "/api/facts":
            return self._handle_facts()
        if urllib.parse.urlparse(self.path).path == "/api/memory":
            return self._handle_memory()
        if urllib.parse.urlparse(self.path).path == "/api/branches":
            return self._handle_branches()
        if urllib.parse.urlparse(self.path).path == "/api/task":
            return self._handle_task()
        if urllib.parse.urlparse(self.path).path == "/api/invariants":
            return self._handle_invariants()
        if urllib.parse.urlparse(self.path).path == "/api/profiles":
            return self._handle_profiles()
        if urllib.parse.urlparse(self.path).path == "/api/mcp":
            return self._handle_mcp()
        self._send_json(404, {"ok": False, "error": "Not Found"})

    # ---------------- Статика ----------------
    def _serve_static_safe(self, rel):
        root_real = os.path.realpath(config.WEB_ROOT)
        full = os.path.realpath(os.path.join(root_real, os.path.normpath(rel)))
        if not (full.startswith(root_real + os.sep) or full == root_real):
            return self._send_bytes(403, "Forbidden", "text/plain")
        if not os.path.isfile(full):
            return self._send_bytes(404, "Not Found", "text/plain")
        ctype, _ = mimetypes.guess_type(full)
        with open(full, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---------------- Обработчики: делегируют всю работу агенту ----------------

    def _maybe_auto_compact(self):
        """Инкрементальное сжатие истории: дописывает вытесненные сообщения.

        Логика: как только история становится больше keep, каждое новое
        вытесненное сообщение ДОПИСЫВАЕТСЯ в summary (Вариант A). Для keep = 5
        сжатие начинается с 6-го сообщения и продолжается по мере вытеснения.
        Summary хранится ОТДЕЛЬНО (compact.summary) и подставляется в
        следующий запрос вместо вытесненной части истории.
        """
        try:
            if not self.session.should_auto_compact():
                return
            head, end = self.session.head_to_compact()
            if not head:
                return
            keep = self.session.get_compact().get("keep", config.COMPACT_KEEP)
            prev_summary = self.session.get_compact().get("summary", "")
            # Дописываем вытесненную часть в существующее summary.
            summary = self.agent.compact_update(prev_summary, head)
            if summary:
                # Сохраняем summary и новую границу: сообщения [0:end) покрыты.
                self.session.apply_summary(summary, upto=end, keep=keep)
                print("[COMPACT] сжатие: +%d сообщ. -> summary (upto=%d)"
                      % (len(head), end), flush=True)
        except Exception as exc:
            # Сжатие не должно ломать основной запрос.
            print("[COMPACT] сжатие не удалось: %s" % exc, flush=True)

    def _handle_ask(self):
        """Принимает запрос пользователя и передаёт его агенту.

        История диалога хранится на сервере (папка session/) и подхватывается
        при каждом запуске. Сервер передаёт агенту историю, выбор моделей и
        температуру, а после успешного ответа добавляет ход в сессию и
        сохраняет её на диск.
        """
        data = self._read_json_body()
        if not data:
            return self._send_json(400, {"ok": False, "error": "Bad JSON."})
        question = str(data.get("question", "")).strip()
        # Выбор моделей пользователем: [{"label": "…", "temperature": 0.7}]
        selected = data.get("models")
        if not isinstance(selected, list) or not selected:
            selected = None

        # РЕЖИМ ОТВЕТА зависит от наличия персон:
        #   * ЕСТЬ активная персона — отвечает ПЕРСОНА, используя СВОЮ модель
        #     (выбор моделей сверху недоступен, но если клиент всё же прислал
        #     список — модель персоны имеет приоритет);
        #   * НЕТ персон — диалог идёт напрямую с ВЫБРАННЫМИ моделями.
        pid, pname, pmodel, pchar, pstyle = self.session.active_profile_attrs()
        if pid:
            # Отвечает персона. Заголовок блока — имя персоны.
            answer_title = pname or "Персона"
            profile = {"name": pname, "character": pchar, "style": pstyle}
            # Модель персоны имеет приоритет; если у персоны модель не задана —
            # используем выбранные сверху модели, а если и их нет — все
            # доступные модели агента (передаём None).
            if pmodel:
                selected = [{"label": pmodel}]
            elif selected is None:
                selected = None  # агент опросит все доступные модели
        else:
            # Персон нет — режим «напрямую с моделями».
            answer_title = None
            profile = None
            if selected is None and not _ServerState.mcp_enabled:
                return self._send_json(200, {
                    "ok": False,
                    "error": "Не выбрана ни одна модель. Включите модель для "
                             "запроса.",
                    "text": "", "html": "", "answers": [],
                })

        # Глобальное ограничение max_tokens (None — не применяется)
        max_tokens = data.get("max_tokens")
        try:
            max_tokens = int(max_tokens)
        except (TypeError, ValueError):
            max_tokens = None

        # Настройки сжатия
        compact = data.get("compact")
        if isinstance(compact, dict):
            # Клиент может прислать свои настройки (enabled/keep) — применяем.
            self.session.set_compact(compact.get("enabled"),
                                     compact.get("keep"),
                                     None)
        # Стратегия управления контекстом (если клиент прислал).
        strategy = data.get("strategy")
        if isinstance(strategy, dict):
            self.session.set_strategy(strategy.get("strategy"),
                                      strategy.get("window"))
        compact = self.session.get_compact()

        # Управление контекстом: применяем АКТИВНУЮ стратегию
        # (sliding / facts / branch) и, поверх неё, сжатие summary.
        history = self.session.get_context_messages()

        # ПРОВЕРКА НАЛИЧИЯ данных в памяти при формировании запроса:
        # если в рабочей/долговременной памяти есть данные — они уже
        # подмешаны в контекст (memory_message) как системное сообщение.
        memory = self.session.memory_state()
        mem_items = (len(memory.get("working") or {})
                     + len(memory.get("longterm") or {}))
        if mem_items:
            print("[MEMORY] в запрос добавлена память: рабочая=%d, "
                  "долговременная=%d (всего %d элементов)"
                  % (len(memory.get("working") or {}),
                     len(memory.get("longterm") or {}), mem_items),
                  flush=True)
        else:
            print("[MEMORY] память пуста — в запрос не добавляется", flush=True)

        # Профиль (персона): характер и характер ответов активного профиля
        # подставляются в системный промпт каждой модели (тон + формат/длина).
        # pid/pname/... и profile уже получены выше (см. выбор режима).

        # СОСТОЯНИЕ ЗАДАЧИ (Task State Machine): формализованный автомат
        # «этап -> шаг -> ожидаемое действие». Передаём его агенту, чтобы он
        # вёл ответ сообразно этапу и после паузы продолжал без повторных
        # объяснений. Если активной задачи нет — None.
        task_state = self.session.get_task_state()
        if not task_state.get("active"):
            task_state = None

        # ИНВАРИАНТЫ: жёсткие правила (архитектура/техрешения/стек/бизнес),
        # которые ассистент не вправе нарушать. Передаём их агенту: он учтёт
        # правила в промпте и выполнит пост-проверку ответа.
        invariants = self.session.get_invariants()
        if invariants:
            print("[INVARIANT] инвариантов в запросе: %d" % len(invariants),
                  flush=True)

        # НАПОМИНАНИЯ: АВТОМАТИЧЕСКИ забираем наступившие напоминания из
        # календаря (MCP-инструмент run_due) и доставляем их в чат ОТДЕЛЬНЫМ
        # сообщением (красный фон) — независимо от того, что спросил
        # пользователь. Так напоминание приходит САМО, без специального
        # запроса «покажи напоминания».
        reminders = _collect_due_reminders()

        # ВЕТКА MCP: если MCP включён в интерфейсе — запрос идёт ЧЕРЕЗ MCP
        # (модель выбирает инструмент, инструмент вызывается на MCP-сервере,
        # его результат возвращается как ответ). Иначе — обычный путь агента.
        if _ServerState.mcp_enabled:
            print("[MCP] запрос идёт через MCP (server=%r, model=%r)"
                  % (_ServerState.mcp_server or "default",
                     _ServerState.mcp_model or "auto"), flush=True)
            tools = []
            if mcp_client is not None:
                listed = mcp_client.mcp_list_tools(
                    server_id=(_ServerState.mcp_server or None))
                if listed.get("ok"):
                    tools = listed.get("tools", [])
                else:
                    print("[MCP] не удалось получить список инструментов: %s"
                          % listed.get("error"), flush=True)
            result = self.agent.answer_via_mcp(
                question, tools,
                model=(_ServerState.mcp_model or None),
                server_id=(_ServerState.mcp_server or None))
        else:
            result = self.agent.answer(question, history, selected,
                                       max_tokens=max_tokens,
                                       compact=compact,
                                       memory=memory,
                                       profile=profile,
                                       answer_title=answer_title,
                                       task_state=task_state,
                                       invariants=invariants)
        if result.get("ok"):
            # По одному ходу на ответ модели с уже готовой разметкой
            self.session.append_turn(question, {
                "role": "assistant",
                "content": result.get("text", ""),
                "html": result.get("html", ""),
                "answers": result.get("answers", []),
                "meta": result.get("meta", ""),
                "usage": result.get("usage"),
            })
            # Стратегия Facts: обновляем блок facts после каждого сообщения
            # пользователя (через GigaChat).
            self._maybe_update_facts(question, result)
            # Счётчики использования памяти: сколько фрагментов ответа
            # заимствовано из рабочей/долговременной памяти (для интерфейса).
            mused = result.get("memory_used") or {}
            self.session.add_memory_usage(mused.get("working", 0),
                                          mused.get("longterm", 0))
            # Сжатие: если появились вытесненные сообщения — дописываем их
            # в summary (инкрементально; работает только при keep > 0).
            self._maybe_auto_compact()
            # СОСТОЯНИЕ ЗАДАЧИ: если задача активна (не на паузе и не done) —
            # пусть агент предложит следующий переход автомата, а сервер
            # применит его ТОЛЬКО если переход корректен (проверка в
            # SessionStore.advance_task). Паузу и завершение клиент задаёт сам.
            self._maybe_advance_task(question, result)
            result["compact"] = self.session.get_compact()
            result["strategy"] = self.session.get_strategy()
            result["facts"] = self.session.get_facts()
            result["branches"] = self.session.branches_state()
            result["memory"] = self.session.memory_state()
            result["task"] = self.session.get_task_state()
            result["invariants"] = self.session.invariants_state()
            # Статистика управления контекстом (сжатых/использованных из
            # summary сообщений) для панели интерфейса.
            result["context"] = self.session.context_stats()
        # Доставляем наступившие напоминания в чат (красным фоном) в ЛЮБОМ
        # случае — даже если ответ модели не удался, напоминание важно.
        if reminders:
            result["reminders"] = reminders
            # В историю сессии НЕ пишем: это разовая доставка, а не ход
            # диалога (иначе напоминания дублировались бы при перезагрузке).
        return self._send_json(200, result)

    def _maybe_advance_task(self, question, result):
        """Продвигает автомат задачи по ходу пользователя (если задача активна).

        Решение о переходе принимает агент (LLM на GigaChat), а сервер
        применяет его только при КОРРЕКТНОСТИ перехода. Задача на паузе не
        двигается — продолжение инициирует пользователь кнопкой «Продолжить».
        """
        try:
            ts = self.session.get_task_state()
            if not ts.get("active") or ts.get("paused") or ts.get("stage") == "done":
                return
            move = self.agent.advance_task(
                ts, question, result.get("text", ""))
            if not move or not move.get("stage"):
                return
            res = self.session.advance_task(
                stage=move.get("stage"),
                step=move.get("step"),
                expected=move.get("expected"),
                note=move.get("note") or "авто-переход по ходу диалога")
            if res.get("ok"):
                print("[TASK] этап -> %s (шаг: %s)"
                      % (res["task"].get("stage"), res["task"].get("step")),
                      flush=True)
            else:
                print("[TASK] переход отклонён: %s" % res.get("error"),
                      flush=True)
        except Exception as exc:
            print("[TASK] авто-переход не удался: %s" % exc, flush=True)

    def _maybe_update_facts(self, question, result):
        """Обновляет блок facts после хода пользователя (стратегия Facts)."""
        try:
            if self.session.get_strategy().get("strategy") != "facts":
                return
            prev = self.session.get_facts()
            last_turn = [
                {"role": "user", "content": str(question)},
                {"role": "assistant", "content": result.get("text", "")},
            ]
            facts = self.agent.update_facts(prev, last_turn)
            if isinstance(facts, dict):
                self.session.set_facts(facts)
        except Exception as exc:
            print("[FACTS] обновление не удалось: %s" % exc, flush=True)

    def _handle_compact(self):
        """Обновляет настройки сжатия сессии (enabled/keep/summary)."""
        data = self._read_json_body()
        if not data:
            return self._send_json(400, {"ok": False, "error": "Bad JSON."})
        enabled = data.get("enabled")
        keep = data.get("keep")
        summary = data.get("summary")
        # Пустой summary из клиента НЕ должен затирать уже сгенерированный
        # на сервере — иначе настройки каждый раз обнуляют сжатие.
        if not summary:
            summary = None
        self.session.set_compact(enabled, keep, summary)
        return self._send_json(200, {
            "ok": True,
            "compact": self.session.get_compact(),
        })

    def _handle_compact_summary(self):
        """Дописывает вытесненную часть истории в summary (по запросу).

        Сжимаем только то, что вышло за пределы последних keep сообщений,
        и ДОПИСЫВАЕМ это в существующее summary (инкрементально, Вариант A).
        Summary сохраняется отдельно и будет подставлено в следующий запрос
        вместо вытесненной части истории.
        """
        data = self._read_json_body()
        if not data:
            return self._send_json(400, {"ok": False, "error": "Bad JSON."})
        keep = data.get("keep", config.COMPACT_KEEP)
        try:
            keep = max(0, int(keep))
        except (TypeError, ValueError):
            keep = config.COMPACT_KEEP
        # Применяем актуальный keep перед вычислением вытесняемой части.
        self.session.set_compact(True, keep, None)
        head, end = self.session.head_to_compact()
        if not head:
            return self._send_json(200, {
                "ok": True, "summary": self.session.get_compact().get("summary", ""),
                "detail": "Нет вытесненных сообщений — сжимать нечего.",
            })
        prev_summary = self.session.get_compact().get("summary", "")
        summary = self.agent.compact_update(prev_summary, head)
        if summary:
            self.session.apply_summary(summary, upto=end, keep=keep)
            return self._send_json(200, {
                "ok": True, "summary": summary,
                "detail": "Summary дополнен (%d сообщ.)." % len(head),
            })
        return self._send_json(200, {
            "ok": False, "summary": self.session.get_compact().get("summary", ""),
            "detail": "Не удалось обновить summary.",
        })

    def _handle_strategy(self):
        """Управление стратегией контекста: none / sliding / facts / branch."""
        data = self._read_json_body()
        if not data:
            return self._send_json(400, {"ok": False, "error": "Bad JSON."})
        state = self.session.set_strategy(data.get("strategy"),
                                          data.get("window"))
        return self._send_json(200, {
            "ok": True,
            "strategy": state,
            "context": self.session.context_stats(),
            "facts": self.session.get_facts(),
            "branches": self.session.branches_state(),
        })

    def _handle_facts(self):
        """Просмотр/редактирование фактов (key-value) стратегии Facts.

        Факты — это данные ПАМЯТИ агента: каждый факт раскладывается в
        ВЫБРАННЫЙ пользователем слой памяти: mem_map = {ключ: "working"|
        "longterm"}. Ключи без явного выбора считаются рабочей памятью.
        Отдельного хранилища facts нет — читать/писать их можно через память.
        """
        data = self._read_json_body()
        if not data:
            return self._send_json(400, {"ok": False, "error": "Bad JSON."})
        facts = data.get("facts")
        if isinstance(facts, dict):
            facts = {str(k): str(v) for k, v in facts.items()}
            mem_map = data.get("mem_map")
            if not isinstance(mem_map, dict):
                mem_map = {}
            # Раскладываем факты по слоям памяти согласно выбору у значения.
            working, longterm = {}, {}
            for k, v in facts.items():
                layer = str(mem_map.get(k, "working")).strip().lower()
                if layer == "longterm":
                    longterm[k] = v
                else:
                    working[k] = v
            try:
                self.session.set_memory_bulk("working", working)
                self.session.set_memory_bulk("longterm", longterm)
            except ValueError as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        return self._send_json(200, {
            "ok": True,
            "facts": self.session.get_facts(),
            "memory": self.session.memory_state(),
        })

    def _handle_memory(self):
        """Память агента: чтение/запись трёх типов (short/working/longterm).

        Тело запроса (все поля необязательны, но action определяет смысл):
            action: "state"  — вернуть снимок всех типов (по умолчанию);
                    "set"    — записать одну пару: {type, key, value};
                    "delete" — удалить ключ: {type, key};
                    "replace"— заменить тип целиком: {type, data:{…}};
                    "clear"  — очистить тип: {type}.
            type:   "working" | "longterm" (для "short" запись запрещена —
                    краткосрочная память ведётся самим диалогом).

        Именно здесь реализован ЯВНЫЙ выбор «что и куда сохраняется» (задание
        B2): тип памяти задаётся вызывающим кодом, слои не пересекаются.
        """
        data = self._read_json_body()
        if not data:
            return self._send_json(400, {"ok": False, "error": "Bad JSON."})
        action = str(data.get("action", "state")).strip().lower()
        mem_type = data.get("type")

        try:
            if action == "set":
                entry = self.session.set_memory_key(
                    mem_type, data.get("key"), data.get("value"))
                return self._send_json(200, {
                    "ok": True, "saved": entry,
                    "memory": self.session.memory_state(),
                })
            if action == "delete":
                removed = self.session.delete_memory_key(mem_type,
                                                         data.get("key"))
                return self._send_json(200, {
                    "ok": True, "removed": removed,
                    "memory": self.session.memory_state(),
                })
            if action == "replace":
                saved = self.session.set_memory_bulk(mem_type, data.get("data"))
                return self._send_json(200, {
                    "ok": True, "saved": saved,
                    "memory": self.session.memory_state(),
                })
            if action == "clear":
                mt = str(mem_type or "").strip()
                if mt not in config.MEMORY_TYPES:
                    return self._send_json(400, {
                        "ok": False,
                        "error": "Неизвестный тип памяти: %r" % (mem_type,),
                    })
                if mt == "short":
                    return self._send_json(400, {
                        "ok": False,
                        "error": "Краткосрочная память (диалог) очищается "
                                 "кнопкой «Новый разговор».",
                    })
                self.session.set_memory_bulk(mt, {})
                return self._send_json(200, {
                    "ok": True, "memory": self.session.memory_state(),
                })
            # action == "state" и всё прочее — отдаём снимок.
            return self._send_json(200, {
                "ok": True, "memory": self.session.memory_state(),
            })
        except ValueError as exc:
            return self._send_json(400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            return self._send_json(500, {"ok": False,
                                         "error": "Ошибка памяти: %s" % exc})

    def _handle_branches(self):
        """Управление ветками: create / switch / delete / rename.

        Ожидаемые поля: action = "create" | "switch" | "delete" | "rename",
        name (для create/rename), count (для create),
        index (для switch/delete/rename).
        """
        data = self._read_json_body()
        if not data:
            return self._send_json(400, {"ok": False, "error": "Bad JSON."})
        action = str(data.get("action", "")).strip().lower()
        if action == "create":
            state = self.session.create_branch(
                name=data.get("name"),
                count=data.get("count"),
                from_checkpoint=bool(data.get("from_checkpoint", True)))
        elif action == "switch":
            state = self.session.switch_branch(data.get("index"))
        elif action == "delete":
            state = self.session.delete_branch(data.get("index"))
        elif action == "rename":
            state = self.session.rename_branch(data.get("index"),
                                               data.get("name"))
        else:
            return self._send_json(400, {
                "ok": False,
                "error": "Неизвестное действие с ветками (action).",
            })
        return self._send_json(200, {
            "ok": True,
            "branches": state,
            "messages": self.session.snapshot(),
        })

    def _handle_task(self):
        """Управление СОСТОЯНИЕМ ЗАДАЧИ (Task State Machine).

        Задача — конечный автомат: этап (planning -> execution -> validation
        -> done) + текущий шаг + ожидаемое действие. Поддерживает ПАУЗУ на
        любом этапе и ПРОДОЛЖЕНИЕ без повторных объяснений (цель и журнал
        сохраняются).

        Ожидаемые поля: action =
            "start"   -> {goal, step?, expected?}   завести новую задачу;
            "advance" -> {stage?, step?, expected?, note?}  корректный переход;
            "pause"   -> {note?}                     поставить на паузу;
            "resume"  -> {note?}                     снять с паузы (продолжить);
            "finish"  -> {note?}                     завершить (из validation);
            "reset"                                  сбросить состояние;
            "state"                                  вернуть снимок (по умолч.).
        """
        data = self._read_json_body()
        if not data:
            # GET-подобный вызов (без тела) — просто отдаём состояние.
            return self._send_json(200, {
                "ok": True, "task": self.session.get_task_state()})
        action = str(data.get("action", "state")).strip().lower()
        try:
            if action == "start":
                task = self.session.start_task(
                    data.get("goal"), step=data.get("step", ""),
                    expected=data.get("expected", ""),
                    stage=data.get("stage", "planning"))
                return self._send_json(200, {"ok": True, "task": task})
            if action == "advance":
                res = self.session.advance_task(
                    stage=data.get("stage"), step=data.get("step"),
                    expected=data.get("expected"), note=data.get("note", ""))
                status = 200 if res.get("ok") else 409
                return self._send_json(status, {
                    "ok": res.get("ok"), "task": res.get("task"),
                    "error": res.get("error")})
            if action == "pause":
                res = self.session.pause_task(data.get("note", ""))
                return self._send_json(200, {"ok": res.get("ok"),
                                             "task": res.get("task")})
            if action == "resume":
                res = self.session.resume_task(data.get("note", ""))
                return self._send_json(200, {"ok": res.get("ok"),
                                             "task": res.get("task")})
            if action == "finish":
                res = self.session.finish_task(data.get("note", ""))
                status = 200 if res.get("ok") else 409
                return self._send_json(status, {
                    "ok": res.get("ok"), "task": res.get("task"),
                    "error": res.get("error")})
            if action == "reset":
                task = self.session.reset_task()
                return self._send_json(200, {"ok": True, "task": task})
            # action == "state" и всё прочее.
            return self._send_json(200, {
                "ok": True, "task": self.session.get_task_state()})
        except ValueError as exc:
            return self._send_json(400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            return self._send_json(500, {
                "ok": False, "error": "Ошибка состояния задачи: %s" % exc})

    def _handle_profiles(self):
        """Управление ПРОФИЛЯМИ (персонами).

        Профиль — именованная персона со СВОЕЙ памятью (рабочая +
        долговременная), своим диалогом и настройками. Атрибуты character
        (характер — тон общения) и style (характер ответов — формат/длина)
        задаются ПРИ СОЗДАНИИ и подставляются в системный промпт каждой
        модели. Переключение профиля заменяет активную память и диалог.

        Ожидаемые поля: action = "create" | "switch" | "update" | "delete";
        id (для switch/update/delete); name, character, style, model (для
        create/update). model — метка модели, которой отвечает персона.
        """
        data = self._read_json_body()
        if not data:
            return self._send_json(400, {"ok": False, "error": "Bad JSON."})
        action = str(data.get("action", "list")).strip().lower()
        try:
            if action == "create":
                state = self.session.create_profile(
                    data.get("name"),
                    character=data.get("character"),
                    style=data.get("style"),
                    model=data.get("model"),
                    activate=bool(data.get("activate", True)))
            elif action == "switch":
                state = self.session.switch_profile(data.get("id"))
            elif action == "update":
                state = self.session.update_profile(
                    data.get("id"),
                    name=data.get("name"),
                    character=data.get("character"),
                    style=data.get("style"),
                    model=data.get("model"))
            elif action == "delete":
                state = self.session.delete_profile(data.get("id"))
            elif action in ("list", "state", ""):
                state = self.session.profiles_state()
            else:
                return self._send_json(400, {
                    "ok": False,
                    "error": "Неизвестное действие с профилями (action).",
                })
        except ValueError as exc:
            return self._send_json(400, {"ok": False, "error": str(exc)})
        # После переключения/удаления активным может стать другой профиль —
        # возвращаем актуальные память и диалог, чтобы интерфейс обновился.
        return self._send_json(200, {
            "ok": True,
            "profiles": state.get("profiles", []),
            "active": state.get("active"),
            "messages": self.session.snapshot(),
            "memory": self.session.memory_state(),
            "branches": self.session.branches_state(),
            "compact": self.session.get_compact(),
            "strategy": self.session.get_strategy(),
            "context": self.session.context_stats(),
        })

    def _handle_invariants(self):
        """Управление ИНВАРИАНТАМИ (правилами, которые нельзя нарушать).

        Инварианты хранятся ОТДЕЛЬНО от диалога (свой раздел профиля) и имеют
        КАТЕГОРИЮ (архитектура / техрешения / стек / бизнес-правила). При
        конфликте запроса с инвариантом ассистент отказывается от решения.

        Ожидаемые поля: action =
            "add"     -> {text, category}            добавить инвариант;
            "update"  -> {id, text?, category?}      изменить инвариант;
            "delete"  -> {id}                         удалить инвариант;
            "replace" -> {invariants:[{text,category,id?}, …]}  заменить весь список;
            "clear"                                   очистить список;
            "state"                                   вернуть снимок (по умолч.).
        """
        data = self._read_json_body() or {}
        action = str(data.get("action", "state")).strip().lower()
        try:
            if action == "add":
                state = self.session.add_invariant(
                    data.get("text"), data.get("category", "business"))
            elif action == "update":
                state = self.session.update_invariant(
                    data.get("id"), text=data.get("text"),
                    category=data.get("category"))
            elif action == "delete":
                removed = self.session.delete_invariant(data.get("id"))
                state = self.session.invariants_state()
                state["removed"] = removed
            elif action == "replace":
                state = self.session.set_invariants(data.get("invariants"))
            elif action == "clear":
                state = self.session.clear_invariants()
            else:  # "state" и всё прочее
                state = self.session.invariants_state()
        except ValueError as exc:
            return self._send_json(400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            return self._send_json(500, {
                "ok": False, "error": "Ошибка инвариантов: %s" % exc})
        return self._send_json(200, state)

    def _handle_mcp(self):
        """Управление MCP: чтение/запись настроек и проверка статуса сервера.

        Тело запроса (все поля необязательны, action определяет смысл):
            action: "state"  — вернуть состояние MCP (по умолчанию);
                    "set"    — сохранить настройки {enabled?, model?};
                    "status" — ПРОВЕРИТЬ статус MCP-сервера: подключиться,
                               получить список инструментов и вернуть
                               результат (для кнопки «Проверить статус»).
            enabled — bool: включать ли использование инструментов MCP;
            model   — метка модели, применяемой при работе с MCP.
        """
        data = self._read_json_body() or {}
        action = str(data.get("action", "state")).strip().lower()

        if action == "set":
            with _ServerState.mcp_lock:
                if "enabled" in data:
                    _ServerState.mcp_enabled = bool(data.get("enabled"))
                if "model" in data:
                    _ServerState.mcp_model = str(data.get("model") or "")
                if "server" in data:
                    _ServerState.mcp_server = str(data.get("server") or "")
                _save_mcp_settings()
            return self._send_json(200, _mcp_state_payload(check=False))

        if action == "status":
            return self._send_json(200, _mcp_state_payload(check=True))

        # action == "state" и всё прочее — отдаём состояние без проверки.
        return self._send_json(200, _mcp_state_payload(check=False))


def create_server(agent, host=None, port=None):
    """Создаёт HTTP-сервер, связанный с конкретным экземпляром агента."""
    if agent is None:
        raise ValueError("create_server: требуется экземпляр агента (Agent).")
    _set_agent(agent)
    # ВАЖНО: проверяем именно None, а не «ложность»: port=0 — валидное
    # значение (ОС сама выберет свободный порт), которое нельзя подменять
    # значением по умолчанию, иначе тесты/инстансы будут конфликтовать
    # на общем порту config.WEB_PORT.
    addr = (host or config.WEB_HOST,
            config.WEB_PORT if port is None else port)
    return ThreadingHTTPServer(addr, WebRequestHandler)


def _open_browser_later(url, delay=1.0):
    def _job():
        time.sleep(delay)
        try:
            webbrowser.open(url)
        except Exception as exc:
            print("[WEB] браузер: %s" % exc)
    threading.Thread(target=_job, daemon=True).start()


def serve(agent_or_key, host=None, port=None, open_page=True, agent=None):
    """Запускает сервер, используя агента в качестве единой сущности.

    agent_or_key — либо строка API-ключа DeepSeek (тогда создаётся агент),
    либо уже готовый экземпляр rtk_app.agent.Agent.
    agent — ещё один способ передать готового агента явно.
    """
    if agent is None:
        agent = agent_or_key

    # Загружаем сохранённые настройки MCP (вкл/выкл + модель).
    _load_mcp_settings()

    # Агент — отдельная сущность, построенная вокруг ключа либо переданная.
    if not isinstance(agent, Agent):
        agent = Agent(agent)

    httpd = create_server(agent, host, port)
    shown_host, shown_port = httpd.server_address[:2]
    url = "http://%s:%s/" % (shown_host, shown_port)
    print("[WEB] Сервер запущен: %s" % url)
    print("[WEB] Агент обслуживает модели: %s" % agent.label)
    if open_page:
        _open_browser_later(url)
    print("[WEB] Остановка: нажмите Ctrl+C.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[WEB] Остановка...")
    finally:
        httpd.server_close()


def main():
    import sys
    from rtk_app.key_store import read_api_key
    try:
        api_key = read_api_key(config.DS_KEY_FILE)
        print("[OK] Ключ DeepSeek прочитан из %s" % config.DS_KEY_FILE)
    except FileNotFoundError:
        # Файла ключа нет: сервер всё равно стартует — интерфейс откроется,
        # а недоступные модели покажут подсказку, как добавить ключ
        # (самодиагностика уже напечатана через rtk_web.py/key_check).
        api_key = ""
        print("[!] Файл %s не найден. DeepSeek-flash в чате станет "
              "недоступен до тех пор, пока вы не впишете ключ в apidpsk.txt."
              % config.DS_KEY_FILE)
    args = sys.argv[1:]
    host, port = config.WEB_HOST, config.WEB_PORT
    if args and args[0].isdigit():
        port = int(args[0])
        if len(args) > 1:
            host = args[1]
    sys.exit(serve(api_key, host, port) or 0)