"""Coordinador del calculo cuadratico distribuido (pub/sub).

Toda la comunicacion va por el broker pub/sub:
  - El cliente publica solicitudes en el topico req.quadratic.
  - El coordinador publica subtareas en task.<worker_id>.
  - Los workers publican resultados en result.<request_id>.
  - El coordinador publica la respuesta final en response.<client_id>.

Hereda la logica de los talleres 1 y 2:
  - Pipeline de tres etapas: sqrt_discriminant -> numerator -> division.
  - Failover por roles (ROLE_PLAN) ante caida de un worker.
  - Modo full_quadratic en un solo worker cuando >= 2 workers caen.

Diseno interno:
  - Un unico Subscriber permanente escucha result.* y heartbeat.* en el
    mismo bucle (evita el slow-joiner por suscripcion tardia).
  - Cuando llega un mensaje result.<req_id> se pone en una cola asociada
    a ese request_id; el hilo que esta resolviendo esa solicitud lo
    consume.

Uso:
    python3 coordinator.py
"""

import queue
import threading
import time

import config
from log import log, banner
from messaging import Publisher, Subscriber


COMP = "COORD"


# ---------------------------------------------------------------------------
# Estado compartido
# ---------------------------------------------------------------------------

_last_hb = {op: 0.0 for op in config.ALL_OPS}
_alive_state = {op: False for op in config.ALL_OPS}   # solo para detectar transiciones
_hb_lock = threading.Lock()

_result_queues = {}
_rq_lock = threading.Lock()


def _is_alive(op):
    with _hb_lock:
        last = _last_hb.get(op, 0.0)
    return (time.time() - last) <= config.HEARTBEAT_DEAD_AFTER


def _alive_ops_now(dead_ops):
    return [op for op in config.ALL_OPS if op not in dead_ops and _is_alive(op)]


def _register_request(request_id):
    q = queue.Queue()
    with _rq_lock:
        _result_queues[request_id] = q
    return q


def _unregister_request(request_id):
    with _rq_lock:
        _result_queues.pop(request_id, None)


# ---------------------------------------------------------------------------
# Listener unico (heartbeats + resultados)
# ---------------------------------------------------------------------------

def _bus_listener():
    sub = Subscriber([
        config.TOPIC_RESULT_PREFIX,
        config.TOPIC_HEARTBEAT_PREFIX,
    ])
    log(f"hilo listener: suscrito a {config.TOPIC_RESULT_PREFIX}* y "
        f"{config.TOPIC_HEARTBEAT_PREFIX}*", COMP)
    while True:
        msg = sub.recv(timeout_ms=1000)
        if msg is None:
            continue
        topic, payload = msg
        if topic.startswith(config.TOPIC_HEARTBEAT_PREFIX):
            worker_id = payload.get("worker_id")
            if worker_id:
                with _hb_lock:
                    _last_hb[worker_id] = time.time()
            continue
        if topic.startswith(config.TOPIC_RESULT_PREFIX):
            req_id = payload.get("request_id")
            log(f"<<< resultado recibido en topic='{topic}' worker={payload.get('worker')} "
                f"stage={payload.get('stage')} ok={payload.get('ok')}", COMP)
            with _rq_lock:
                q = _result_queues.get(req_id)
            if q is not None:
                q.put(payload)
            else:
                log(f"    (resultado para req={req_id} sin handler activo, descartado)", COMP)


def _alive_watchdog():
    """Vigila los heartbeats y loguea transiciones VIVO <-> CAIDO."""
    while True:
        time.sleep(1.0)
        for op in config.ALL_OPS:
            alive = _is_alive(op)
            with _hb_lock:
                prev = _alive_state.get(op, False)
                _alive_state[op] = alive
            if alive and not prev:
                log(f"@@@ transicion: {op} ahora esta VIVO (heartbeat detectado)",
                    COMP)
            elif not alive and prev:
                log(f"@@@ transicion: {op} ahora esta CAIDO (sin heartbeat por "
                    f">{config.HEARTBEAT_DEAD_AFTER}s)", COMP)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _wait_result(q, expected_worker, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        remaining = deadline - time.time()
        try:
            payload = q.get(timeout=max(0.05, remaining))
        except queue.Empty:
            return None
        if expected_worker is None or payload.get("worker") == expected_worker:
            return payload
        # Resultado rezagado de otro worker: lo ignoramos y seguimos esperando.
    return None


def _run_stage(pub, q, request_id, stage, payload, dead_ops):
    log(f"== Etapa '{stage}' (req={request_id[:8]})  plan = {config.ROLE_PLAN[stage]}",
        COMP)
    for op_name in config.ROLE_PLAN[stage]:
        if op_name in dead_ops:
            log(f"   {op_name}: ya marcado en dead_ops, salto", COMP)
            continue
        if not _is_alive(op_name):
            log(f"   {op_name}: sin heartbeat reciente -> dead_ops", COMP)
            dead_ops.add(op_name)
            continue
        task = dict(payload)
        task["request_id"] = request_id
        task["stage"] = stage
        log(f"   --> publicando tarea en topic='{config.topic_task(op_name)}' "
            f"({stage} -> {op_name})", COMP)
        pub.send(config.topic_task(op_name), task)
        result = _wait_result(q, op_name, config.STAGE_TIMEOUT)
        if result is None:
            log(f"   {op_name}: TIMEOUT ({config.STAGE_TIMEOUT}s sin "
                f"resultado) -> dead_ops", COMP)
            dead_ops.add(op_name)
            continue
        log(f"   OK: {op_name} respondio (ok={result.get('ok')})", COMP)
        return result, op_name
    log(f"   AGOTADO: ningun candidato respondio para '{stage}'", COMP)
    return None, None


def _try_full_quadratic(pub, q, request_id, a, b, c, dead_ops):
    alive = _alive_ops_now(dead_ops)
    if len(alive) != 1:
        return None, None
    op_name = alive[0]
    log(f"== modo SINGLE_NODE_FALLBACK -> delegando todo el calculo a {op_name}",
        COMP)
    pub.send(config.topic_task(op_name), {
        "request_id": request_id,
        "stage": "full_quadratic",
        "a": a, "b": b, "c": c,
    })
    result = _wait_result(q, op_name, config.STAGE_TIMEOUT)
    if result is None:
        log(f"   {op_name}: TIMEOUT en full_quadratic -> dead_ops", COMP)
        dead_ops.add(op_name)
        return None, None
    return result, op_name


def _solve(pub, request):
    request_id = request.get("request_id")
    a = float(request.get("a"))
    b = float(request.get("b"))
    c = float(request.get("c"))
    dead_ops = set(op for op in config.ALL_OPS if not _is_alive(op))

    log("-" * 70, COMP)
    log(f"** procesando solicitud req={request_id[:8]}  a={a} b={b} c={c}", COMP)
    log(f"   estado inicial: vivos={_alive_ops_now(dead_ops)}  caidos={sorted(dead_ops)}",
        COMP)

    q = _register_request(request_id)
    try:
        if abs(a) < config.EPS:
            log(f"   validacion: a=0 -> error", COMP)
            return {"ok": False, "error": "a no puede ser 0"}

        if len(dead_ops) >= 2:
            log(f"   ya hay {len(dead_ops)} caidos al iniciar -> single_node directo",
                COMP)
            full, who = _try_full_quadratic(pub, q, request_id, a, b, c, dead_ops)
            if full is None or not full.get("ok"):
                return {"ok": False,
                        "error": (full or {}).get("error", "no hay nodos disponibles"),
                        "dead_ops": sorted(dead_ops)}
            return {"ok": True, "x1": full["x1"], "x2": full["x2"],
                    "mode": "single_node_fallback",
                    "trace": f"full_quadratic:{who}",
                    "dead_ops": sorted(dead_ops)}

        # ETAPA 1
        r1, who1 = _run_stage(pub, q, request_id, "sqrt_discriminant",
                              {"a": a, "b": b, "c": c}, dead_ops)
        if len(dead_ops) >= 2 or r1 is None:
            full, who = _try_full_quadratic(pub, q, request_id, a, b, c, dead_ops)
            if full is None or not full.get("ok"):
                return {"ok": False,
                        "error": (full or {}).get("error",
                                                  "Perdona la demora, intenta mas tarde"),
                        "dead_ops": sorted(dead_ops)}
            return {"ok": True, "x1": full["x1"], "x2": full["x2"],
                    "mode": "single_node_fallback",
                    "trace": f"full_quadratic:{who}",
                    "dead_ops": sorted(dead_ops)}
        if not r1.get("ok"):
            return {"ok": False, "error": r1.get("error"),
                    "dead_ops": sorted(dead_ops)}

        # ETAPA 2
        r2, who2 = _run_stage(pub, q, request_id, "numerator",
                              {"b": b, "sqrt_d": r1["sqrt_d"]}, dead_ops)
        if len(dead_ops) >= 2 or r2 is None:
            full, who = _try_full_quadratic(pub, q, request_id, a, b, c, dead_ops)
            if full is None or not full.get("ok"):
                return {"ok": False,
                        "error": (full or {}).get("error",
                                                  "Perdona la demora, intenta mas tarde"),
                        "dead_ops": sorted(dead_ops)}
            return {"ok": True, "x1": full["x1"], "x2": full["x2"],
                    "mode": "single_node_fallback",
                    "trace": f"full_quadratic:{who}",
                    "dead_ops": sorted(dead_ops)}
        if not r2.get("ok"):
            return {"ok": False, "error": r2.get("error"),
                    "dead_ops": sorted(dead_ops)}

        # ETAPA 3
        r3, who3 = _run_stage(pub, q, request_id, "division",
                              {"a": a, "num_plus": r2["num_plus"],
                               "num_minus": r2["num_minus"]}, dead_ops)
        if len(dead_ops) >= 2 or r3 is None:
            full, who = _try_full_quadratic(pub, q, request_id, a, b, c, dead_ops)
            if full is None or not full.get("ok"):
                return {"ok": False,
                        "error": (full or {}).get("error",
                                                  "Perdona la demora, intenta mas tarde"),
                        "dead_ops": sorted(dead_ops)}
            return {"ok": True, "x1": full["x1"], "x2": full["x2"],
                    "mode": "single_node_fallback",
                    "trace": f"full_quadratic:{who}",
                    "dead_ops": sorted(dead_ops)}
        if not r3.get("ok"):
            return {"ok": False, "error": r3.get("error"),
                    "dead_ops": sorted(dead_ops)}

        return {"ok": True, "x1": r3["x1"], "x2": r3["x2"],
                "mode": "pipeline",
                "trace": f"sqrt:{who1} -> num:{who2} -> div:{who3}",
                "dead_ops": sorted(dead_ops)}
    finally:
        _unregister_request(request_id)


# Cache para reenviar respuesta si el cliente reintenta (slow-joiner).
_response_cache = {}
_response_cache_order = []
_response_cache_lock = threading.Lock()
_RESPONSE_CACHE_MAX = 1024


def _cache_response(request_id, client_id, response):
    with _response_cache_lock:
        _response_cache[request_id] = (client_id, response)
        _response_cache_order.append(request_id)
        if len(_response_cache_order) > _RESPONSE_CACHE_MAX:
            old = _response_cache_order.pop(0)
            _response_cache.pop(old, None)


def _get_cached_response(request_id):
    with _response_cache_lock:
        return _response_cache.get(request_id)


def _handle_request(pub, request):
    response = _solve(pub, request)
    response["request_id"] = request.get("request_id")
    client_id = request.get("client_id", "unknown")
    _cache_response(request.get("request_id"), client_id, response)
    pub.send(config.topic_response(client_id), response)
    log(f">>> RESPUESTA publicada en topic='{config.topic_response(client_id)}' "
        f"ok={response.get('ok')} mode={response.get('mode')} "
        f"trace='{response.get('trace')}' dead_ops={response.get('dead_ops')}",
        COMP)
    log("-" * 70, COMP)


def run():
    banner(f"COORDINADOR arrancando", COMP)
    log(f"conectando a brokers: {config.broker_pub_endpoints()}", COMP)
    pub = Publisher()
    req_sub = Subscriber([config.TOPIC_REQ])
    log(f"suscrito a topico de solicitudes: {config.TOPIC_REQ}", COMP)
    log(f"plan de roles: {config.ROLE_PLAN}", COMP)

    threading.Thread(target=_bus_listener, daemon=True).start()
    threading.Thread(target=_alive_watchdog, daemon=True).start()

    log(f"esperando solicitudes...", COMP)

    seen_requests = set()
    seen_order = []
    SEEN_MAX = 1024

    while True:
        msg = req_sub.recv(timeout_ms=1000)
        if msg is None:
            continue
        _topic, payload = msg
        req_id = payload.get("request_id")
        if req_id in seen_requests:
            cached = _get_cached_response(req_id)
            if cached is not None:
                client_id, response = cached
                pub.send(config.topic_response(client_id), response)
                log(f"(reintento del cliente) reenvio respuesta cacheada a "
                    f"{client_id} req={req_id[:8]}", COMP)
            continue
        seen_requests.add(req_id)
        seen_order.append(req_id)
        if len(seen_order) > SEEN_MAX:
            seen_requests.discard(seen_order.pop(0))
        log(f"+++ NUEVA SOLICITUD recibida en topic='{config.TOPIC_REQ}' "
            f"client={payload.get('client_id')} req={req_id[:8]}", COMP)
        threading.Thread(target=_handle_request, args=(pub, payload),
                         daemon=True).start()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log(f"coordinador detenido por el usuario", COMP)
