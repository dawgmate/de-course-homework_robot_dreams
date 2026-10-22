# Завдання 1-2: чиста логіка перетворення (без Kafka, без мережі).
# Реалізуйте обидві функції. Їх перевіряє tests/test_transform.py.
from datetime import datetime

# Дано, не редагувати. П'ять типів подій, які лишаємо; решта відкидається.
ALLOWED_TYPES = (
    "PushEvent",
    "IssuesEvent",
    "PullRequestEvent",
    "WatchEvent",
    "IssueCommentEvent",
)


def event_filter(event: dict) -> bool:
    """Завдання 2 (12 балів).

    Поверніть True ТІЛЬКИ для подій, які:
      * мають `type` зі списку ALLOWED_TYPES (5 типів), І
      * є публічними (`public` == True; якщо ключа немає — вважайте публічною).
    Усі інші події (інші типи, приватні) → False.
    """
    return event.get("type") in ALLOWED_TYPES and event.get("public", True) is True


def flatten_event(event: dict) -> dict:
    """Завдання 1 (18 балів).

    Розпластайте вкладену подію GitHub Archive у плоский запис із полями:
      id                   -> event["id"]                     (str)
      event_type           -> event["type"]                   (str)
      created_at           -> epoch-мілісекунди               (int)   див. _to_millis
      actor_login          -> event["actor"]["login"]         (str)
      repo_name            -> event["repo"]["name"]            (str)
      public               -> event.get("public", True)       (bool)
      payload_action       -> payload.get("action")           (str | None)
      payload_ref          -> payload.get("ref")              (str | None)
      payload_commit_count -> к-сть коммітів у payload         (int | None)

    payload змінюється залежно від типу події, тому три останні поля nullable:
    повертайте None, коли джерельна подія їх не містить. Для payload_commit_count
    візьміть довжину списку payload["commits"], якщо він є, інакше None.
    """
    payload = event.get("payload") or {}
    commits = payload.get("commits")

    return {
        "id": event["id"],
        "event_type": event["type"],
        "created_at": _to_millis(event["created_at"]),
        "actor_login": event["actor"]["login"],
        "repo_name": event["repo"]["name"],
        "public": event.get("public", True),
        "payload_action": payload.get("action"),
        "payload_ref": payload.get("ref"),
        "payload_commit_count": len(commits) if commits is not None else None,
    }


def _to_millis(created_at: str) -> int:
    """ISO-8601 ('2024-01-15T14:00:01Z') -> epoch-мілісекунди (int). Готова функція."""
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)
