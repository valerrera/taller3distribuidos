"""Cliente del sistema de calculo cuadratico (pub/sub).

Solo muestra al usuario lo esencial: la solicitud que envia y la respuesta
que recibe. Internamente publica en req.quadratic y se suscribe a
response.<client_id>; estos detalles no se imprimen porque el cliente no
debe enterarse del estado interno del sistema (workers caidos, broker
caido, reintentos, etc.).

Uso:
    python3 client.py                 # interactivo (pide a, b, c por consola)
    python3 client.py 1 -3 2          # one-shot
    python3 client.py --id miCliente  # personaliza el client_id
"""

import argparse
import sys
import time
import uuid

import config
from messaging import Publisher, Subscriber


def pedir_numero(mensaje):
    while True:
        valor = input(mensaje).strip()
        try:
            return float(valor)
        except ValueError:
            print(f"  '{valor}' no es un numero valido. Intente de nuevo.")


def solve(client_id, a, b, c, timeout=None):
    timeout = timeout if timeout is not None else config.REQUEST_TIMEOUT
    request_id = str(uuid.uuid4())
    pub = Publisher()
    sub = Subscriber([config.topic_response(client_id)])
    try:
        deadline = time.time() + timeout
        # Reintento silencioso cada 2 s — el cliente no necesita saberlo.
        next_send = 0.0
        while time.time() < deadline:
            now = time.time()
            if now >= next_send:
                pub.send(config.TOPIC_REQ, {
                    "request_id": request_id,
                    "client_id": client_id,
                    "a": a, "b": b, "c": c,
                })
                next_send = now + 2.0
            ms_left = int((min(next_send, deadline) - time.time()) * 1000)
            msg = sub.recv(timeout_ms=max(50, ms_left))
            if msg is None:
                continue
            _topic, payload = msg
            if payload.get("request_id") == request_id:
                return payload
        return {"ok": False, "error": "Tiempo de espera agotado. Intenta mas tarde."}
    finally:
        pub.close()
        sub.close()


def _print_solicitud(a, b, c):
    print(f"\n>>> Solicitud: resolver  {a}x^2 + ({b})x + {c} = 0")


def _print_response(resp):
    if resp.get("ok"):
        print(f"<<< Respuesta:")
        print(f"      x1 = {resp.get('x1')}")
        print(f"      x2 = {resp.get('x2')}")
    else:
        print(f"<<< Error: {resp.get('error')}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("a", nargs="?", type=float)
    parser.add_argument("b", nargs="?", type=float)
    parser.add_argument("c", nargs="?", type=float)
    parser.add_argument("--id", dest="client_id",
                        default=f"client-{uuid.uuid4().hex[:6]}")
    parser.add_argument("--timeout", type=float, default=None)
    args = parser.parse_args()

    print(f"=== Cliente cuadratico ({args.client_id}) ===")

    if args.a is not None and args.b is not None and args.c is not None:
        _print_solicitud(args.a, args.b, args.c)
        resp = solve(args.client_id, args.a, args.b, args.c, args.timeout)
        _print_response(resp)
        return 0 if resp.get("ok") else 1

    while True:
        try:
            a = pedir_numero("a: ")
            b = pedir_numero("b: ")
            c = pedir_numero("c: ")
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        _print_solicitud(a, b, c)
        resp = solve(args.client_id, a, b, c, args.timeout)
        _print_response(resp)
        cont = input("\nOtra ecuacion? (s/N): ").strip().lower()
        if cont != "s":
            return 0


if __name__ == "__main__":
    sys.exit(main())
