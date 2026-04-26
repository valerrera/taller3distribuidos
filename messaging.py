"""Helpers comunes para publicar y suscribirse a traves del broker dual.

Patron pub/sub con dos brokers en paralelo:
  - Cada socket PUB hace connect a TODOS los brokers (XSUB de cada broker).
  - Cada socket SUB hace connect a TODOS los brokers (XPUB de cada broker).
Si un broker cae, el otro sigue entregando los mensajes. Como un mismo
mensaje puede llegar dos veces (uno por broker), los receptores deduplican
por (topico, payload['msg_id']).
"""

import json
import time
import uuid

import zmq

import config


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------

class Publisher:
    """Socket PUB conectado a los dos brokers en paralelo."""

    def __init__(self, ctx=None):
        self.ctx = ctx or zmq.Context.instance()
        self.sock = self.ctx.socket(zmq.PUB)
        # Evitar bloqueos si un broker esta caido
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.setsockopt(zmq.SNDHWM, 1000)
        for endpoint in config.broker_pub_endpoints():
            self.sock.connect(endpoint)
        # ZMQ pub/sub tiene "slow joiner": dejamos un instante para que el
        # broker registre la conexion antes de publicar.
        time.sleep(config.SLOW_JOINER_GRACE)

    def send(self, topic, payload):
        if "msg_id" not in payload:
            payload["msg_id"] = str(uuid.uuid4())
        if "ts" not in payload:
            payload["ts"] = time.time()
        body = json.dumps(payload).encode("utf-8")
        self.sock.send_multipart([topic.encode("utf-8"), body])

    def close(self):
        self.sock.close(0)


# ---------------------------------------------------------------------------
# Subscriber
# ---------------------------------------------------------------------------

class Subscriber:
    """Socket SUB conectado a los dos brokers, con deduplicacion por msg_id."""

    def __init__(self, topics, ctx=None, dedupe_capacity=2048):
        self.ctx = ctx or zmq.Context.instance()
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.setsockopt(zmq.RCVHWM, 1000)
        for endpoint in config.broker_sub_endpoints():
            self.sock.connect(endpoint)
        for t in topics:
            self.sock.setsockopt(zmq.SUBSCRIBE, t.encode("utf-8"))
        self._seen = set()
        self._seen_order = []
        self._dedupe_capacity = dedupe_capacity

    def subscribe(self, topic):
        self.sock.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))

    def unsubscribe(self, topic):
        self.sock.setsockopt(zmq.UNSUBSCRIBE, topic.encode("utf-8"))

    def recv(self, timeout_ms=1000):
        """Bloquea hasta timeout_ms y devuelve (topic, payload) o None."""
        deadline = time.time() + timeout_ms / 1000.0
        while True:
            remaining = max(0, int((deadline - time.time()) * 1000))
            if self.sock.poll(remaining) == 0:
                return None
            topic_b, body_b = self.sock.recv_multipart()
            topic = topic_b.decode("utf-8")
            payload = json.loads(body_b.decode("utf-8"))
            msg_id = payload.get("msg_id")
            if msg_id is not None:
                key = (topic, msg_id)
                if key in self._seen:
                    # Duplicado por broker redundante: ignorar y seguir esperando
                    continue
                self._seen.add(key)
                self._seen_order.append(key)
                if len(self._seen_order) > self._dedupe_capacity:
                    old = self._seen_order.pop(0)
                    self._seen.discard(old)
            return topic, payload

    def close(self):
        self.sock.close(0)
