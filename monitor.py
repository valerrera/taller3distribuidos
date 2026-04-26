"""Monitor: suscribe a TODOS los topicos del sistema y los imprime.

Util para sustentar el trabajo, ya que demuestra a simple vista la
estructura de topicos y la actividad en tiempo real del bus pub/sub.

Uso:
    python3 monitor.py
"""

import time

import config
from log import log, banner
from messaging import Subscriber


PREFIXES = [
    config.TOPIC_REQ,
    config.TOPIC_TASK_PREFIX,
    config.TOPIC_RESULT_PREFIX,
    config.TOPIC_RESPONSE_PREFIX,
    config.TOPIC_HEARTBEAT_PREFIX,
    config.TOPIC_SYSTEM_BROKER_ALIVE,
]

COMP = "MONITOR"


def main():
    banner(f"MONITOR del bus pub/sub", COMP)
    sub = Subscriber(PREFIXES)
    log("suscrito a:", COMP)
    for p in PREFIXES:
        log(f"   - {p}*", COMP)
    log("escuchando...", COMP)
    log("-" * 70, COMP)

    while True:
        msg = sub.recv(timeout_ms=1000)
        if msg is None:
            continue
        topic, payload = msg
        # Compactar payload omitiendo metadatos.
        compact = {k: v for k, v in payload.items() if k not in ("msg_id", "ts")}
        if topic == config.TOPIC_SYSTEM_BROKER_ALIVE:
            log(f"[broker_alive]  {topic}  {compact}", COMP)
        elif topic.startswith(config.TOPIC_HEARTBEAT_PREFIX):
            log(f"[heartbeat]     {topic}  {compact}", COMP)
        elif topic == config.TOPIC_REQ:
            log(f"[req-cliente]   {topic}  {compact}", COMP)
        elif topic.startswith(config.TOPIC_TASK_PREFIX):
            log(f"[task->worker]  {topic}  {compact}", COMP)
        elif topic.startswith(config.TOPIC_RESULT_PREFIX):
            log(f"[result->coord] {topic}  {compact}", COMP)
        elif topic.startswith(config.TOPIC_RESPONSE_PREFIX):
            log(f"[resp->cliente] {topic}  {compact}", COMP)
        else:
            log(f"[?]             {topic}  {compact}", COMP)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log(f"monitor detenido", COMP)
