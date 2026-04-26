"""Worker (servidor de operacion) basado en pub/sub.

Cada worker:
  - Se suscribe a su topico privado (task.<worker_id>) para recibir tareas.
  - Publica resultados en result.<request_id>.
  - Publica un latido periodico en heartbeat.<worker_id>.

Operaciones soportadas (las mismas de los talleres 1 y 2):
  - sqrt_discriminant
  - numerator
  - division
  - full_quadratic   (modo contingencia: el worker hace todo el calculo)

Uso:
    python3 worker.py op1
    python3 worker.py op2
    python3 worker.py op3
    python3 worker.py opN   # cualquier id nuevo es valido (escalabilidad)
"""

import math
import sys
import threading
import time

import config
from log import log, banner
from messaging import Publisher, Subscriber


def _calc_sqrt_discriminant(a, b, c, comp):
    log(f"calculando discriminante: {b}^2 - 4*{a}*{c}", comp)
    disc = b * b - 4 * a * c
    log(f"  disc = {disc}", comp)
    if disc < 0:
        log(f"  discriminante negativo -> no hay raices reales", comp)
        return {"ok": False, "error": "discriminante negativo (no hay raices reales)"}
    sqrt_d = math.sqrt(disc)
    log(f"  sqrt_d = {sqrt_d}", comp)
    return {"ok": True, "disc": disc, "sqrt_d": sqrt_d}


def _calc_numerator(b, sqrt_d, comp):
    log(f"calculando numeradores: -({b}) +/- {sqrt_d}", comp)
    res = {"ok": True, "num_plus": (-b) + sqrt_d, "num_minus": (-b) - sqrt_d}
    log(f"  num_plus  = {res['num_plus']}", comp)
    log(f"  num_minus = {res['num_minus']}", comp)
    return res


def _calc_division(a, num_plus, num_minus, comp):
    den = 2 * a
    log(f"calculando division: numeradores / (2*{a}) = numeradores / {den}", comp)
    if abs(den) < config.EPS:
        log(f"  denominador 0 -> error", comp)
        return {"ok": False, "error": "division por cero"}
    res = {"ok": True, "x1": num_plus / den, "x2": num_minus / den}
    log(f"  x1 = {res['x1']}", comp)
    log(f"  x2 = {res['x2']}", comp)
    return res


def _calc_full_quadratic(a, b, c, comp):
    log(f"modo CONTINGENCIA: hago las 3 etapas yo solo", comp)
    if abs(a) < config.EPS:
        log(f"  a = 0 -> no es ecuacion cuadratica", comp)
        return {"ok": False, "error": "a no puede ser 0"}
    disc = b * b - 4 * a * c
    log(f"  disc = {disc}", comp)
    if disc < 0:
        log(f"  discriminante negativo -> no hay raices reales", comp)
        return {"ok": False, "error": "discriminante negativo (no hay raices reales)"}
    sqrt_d = math.sqrt(disc)
    den = 2 * a
    res = {"ok": True, "x1": ((-b) + sqrt_d) / den, "x2": ((-b) - sqrt_d) / den}
    log(f"  sqrt_d = {sqrt_d}, den = {den}", comp)
    log(f"  x1 = {res['x1']}", comp)
    log(f"  x2 = {res['x2']}", comp)
    return res


def _heartbeat_loop(pub, worker_id, comp):
    tick = 0
    while True:
        pub.send(config.topic_heartbeat(worker_id),
                 {"worker_id": worker_id, "ts": time.time(), "tick": tick})
        log(f"latido #{tick} -> {config.topic_heartbeat(worker_id)}", comp)
        tick += 1
        time.sleep(config.HEARTBEAT_INTERVAL)


def run(worker_id):
    comp = worker_id.upper()
    banner(f"WORKER {worker_id} arrancando", comp)
    log(f"conectando a brokers: {config.broker_pub_endpoints()}", comp)
    pub = Publisher()
    sub = Subscriber([config.topic_task(worker_id)])
    log(f"suscrito a topico: {config.topic_task(worker_id)}", comp)
    log(f"publicare resultados en: {config.TOPIC_RESULT_PREFIX}<request_id>", comp)
    log(f"publicare heartbeats en: {config.topic_heartbeat(worker_id)} "
        f"cada {config.HEARTBEAT_INTERVAL}s", comp)

    threading.Thread(target=_heartbeat_loop, args=(pub, worker_id, comp),
                     daemon=True).start()

    log(f"esperando tareas...", comp)
    log("-" * 70, comp)

    request_count = 0
    while True:
        msg = sub.recv(timeout_ms=1000)
        if msg is None:
            continue
        topic, payload = msg
        request_id = payload.get("request_id")
        stage = payload.get("stage")
        request_count += 1
        datos = {k: v for k, v in payload.items()
                 if k not in ("msg_id", "ts", "request_id", "stage")}
        log(f">>> TAREA #{request_count} recibida en topic='{topic}'", comp)
        log(f"    request_id = {request_id}", comp)
        log(f"    stage      = {stage}", comp)
        log(f"    datos      = {datos}", comp)

        if stage == "sqrt_discriminant":
            result = _calc_sqrt_discriminant(payload["a"], payload["b"], payload["c"], comp)
        elif stage == "numerator":
            result = _calc_numerator(payload["b"], payload["sqrt_d"], comp)
        elif stage == "division":
            result = _calc_division(payload["a"], payload["num_plus"], payload["num_minus"], comp)
        elif stage == "full_quadratic":
            result = _calc_full_quadratic(payload["a"], payload["b"], payload["c"], comp)
        else:
            log(f"stage desconocida: {stage}", comp)
            result = {"ok": False, "error": f"stage desconocida: {stage}"}

        result.update({
            "request_id": request_id,
            "stage": stage,
            "worker": worker_id,
        })
        out_topic = config.topic_result(request_id)
        pub.send(out_topic, result)
        log(f"<<< RESULTADO publicado en topic='{out_topic}' "
            f"(ok={result.get('ok')})", comp)
        log("-" * 70, comp)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("uso: python3 worker.py <worker_id>   (ej: op1)")
    try:
        run(sys.argv[1])
    except KeyboardInterrupt:
        log(f"worker detenido por el usuario", sys.argv[1].upper())
