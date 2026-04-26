"""Broker XSUB/XPUB en nodo separado.

Uso:
    python3 broker.py primary    # broker primario (puertos 5559/5560)
    python3 broker.py backup     # broker backup   (puertos 5561/5562)

Implementa un proxy manual (en vez de zmq.proxy) para poder loguear cada
suscripcion, des-suscripcion y mensaje que pasa por el broker, dando total
visibilidad de la actividad del bus pub/sub. Tambien publica un latido
periodico en system.broker.alive.
"""

import json
import sys
import threading
import time

import zmq

import config
from log import log, banner


def _heartbeat_loop(pub_endpoint, broker_id):
    """El broker se publica a si mismo en system.broker.alive."""
    ctx = zmq.Context.instance()
    pub = ctx.socket(zmq.PUB)
    pub.connect(pub_endpoint)
    time.sleep(config.SLOW_JOINER_GRACE)
    tick = 0
    while True:
        msg = json.dumps({"broker_id": broker_id, "ts": time.time(), "tick": tick,
                          "msg_id": f"hb-{broker_id}-{tick}"})
        pub.send_multipart([
            config.TOPIC_SYSTEM_BROKER_ALIVE.encode(),
            msg.encode(),
        ])
        log(f"latido propio #{tick} publicado en {config.TOPIC_SYSTEM_BROKER_ALIVE}",
            f"BROKER:{broker_id}")
        tick += 1
        time.sleep(config.BROKER_HEARTBEAT_INTERVAL)


def _proxy_loop(frontend, backend, role):
    """Proxy manual con logging de subs y mensajes."""
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    msg_count = 0
    sub_counts = {}        # topico -> numero de suscriptores activos
    last_stats = time.time()

    while True:
        events = dict(poller.poll(1000))

        # Lado frontend (XSUB): publicadores publican mensajes -> reenviar a XPUB
        if frontend in events:
            parts = frontend.recv_multipart()
            backend.send_multipart(parts)
            msg_count += 1
            if parts:
                topic = parts[0].decode("utf-8", errors="replace")
                # Para no inundar, omitimos los mensajes de heartbeat de workers
                # y broker. Se siguen contando en msg_count.
                if not topic.startswith(config.TOPIC_HEARTBEAT_PREFIX) \
                        and topic != config.TOPIC_SYSTEM_BROKER_ALIVE:
                    log(f"PUB->SUB  topic='{topic}'  (msg #{msg_count})",
                        f"BROKER:{role}")

        # Lado backend (XPUB): los suscriptores envian mensajes de SUB/UNSUB
        if backend in events:
            parts = backend.recv_multipart()
            # Reenviamos al XSUB (asi los publicadores tambien saben).
            frontend.send_multipart(parts)
            if parts and parts[0]:
                first = parts[0][0]
                topic = parts[0][1:].decode("utf-8", errors="replace")
                if first == 1:
                    sub_counts[topic] = sub_counts.get(topic, 0) + 1
                    log(f"+SUB    topic='{topic}'   (suscriptores a este "
                        f"topico = {sub_counts[topic]})",
                        f"BROKER:{role}")
                elif first == 0:
                    sub_counts[topic] = max(0, sub_counts.get(topic, 0) - 1)
                    log(f"-UNSUB  topic='{topic}'   (suscriptores a este "
                        f"topico = {sub_counts[topic]})",
                        f"BROKER:{role}")

        # Estadisticas periodicas cada 10 s
        now = time.time()
        if now - last_stats > 10:
            activos = sum(1 for c in sub_counts.values() if c > 0)
            log(f"== stats == mensajes_relayed={msg_count}  "
                f"topicos_con_suscriptores={activos}  "
                f"total_suscripciones={sum(sub_counts.values())}",
                f"BROKER:{role}")
            last_stats = now


def run(role):
    if role == "primary":
        xsub_port = config.BROKER_PRIMARY_XSUB_PORT
        xpub_port = config.BROKER_PRIMARY_XPUB_PORT
    elif role == "backup":
        xsub_port = config.BROKER_BACKUP_XSUB_PORT
        xpub_port = config.BROKER_BACKUP_XPUB_PORT
    else:
        raise SystemExit(f"role debe ser 'primary' o 'backup', no {role!r}")

    ctx = zmq.Context.instance()
    frontend = ctx.socket(zmq.XSUB)
    frontend.bind(f"tcp://*:{xsub_port}")
    backend = ctx.socket(zmq.XPUB)
    backend.bind(f"tcp://*:{xpub_port}")
    # XPUB devuelve un mensaje cuando se suscribe alguien — necesario para
    # que el bucle proxy registre las suscripciones.
    backend.setsockopt(zmq.XPUB_VERBOSE, 1)

    banner(f"BROKER {role.upper()} arrancando", f"BROKER:{role}")
    log(f"escuchando publicadores en  tcp://*:{xsub_port}  (XSUB)",
        f"BROKER:{role}")
    log(f"sirviendo suscriptores en  tcp://*:{xpub_port}  (XPUB)",
        f"BROKER:{role}")
    log(f"latido propio en  {config.TOPIC_SYSTEM_BROKER_ALIVE}  cada "
        f"{config.BROKER_HEARTBEAT_INTERVAL}s",
        f"BROKER:{role}")

    threading.Thread(
        target=_heartbeat_loop,
        args=(f"tcp://127.0.0.1:{xsub_port}", role),
        daemon=True,
    ).start()

    try:
        _proxy_loop(frontend, backend, role)
    except KeyboardInterrupt:
        log(f"detenido por el usuario", f"BROKER:{role}")
    finally:
        frontend.close(0)
        backend.close(0)
        ctx.term()


if __name__ == "__main__":
    role = sys.argv[1] if len(sys.argv) > 1 else "primary"
    run(role)
