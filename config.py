"""Configuracion compartida del sistema distribuido pub/sub.

============================================================================
ASIGNACION DE MAQUINAS DEL GRUPO
============================================================================
   Worker 1     -> Valeria   10.43.99.136
   Worker 2     -> Xiomara   10.43.99.139
   Worker 3     -> Alviz     10.43.97.155
   Coordinador  -> David     10.43.97.251   (ademas: Broker PRIMARIO aqui)
   Cliente      -> Carol     10.43.100.92   (ademas: Broker BACKUP aqui)

Los dos brokers se ubican en las maquinas de Coordinador y Cliente porque
son las que tienen menos carga de calculo. Cada uno corre como proceso
independiente del rol principal de su maquina.
"""

import os


# ---------------------------------------------------------------------------
# IPs y puertos de los dos brokers (XSUB/XPUB)
# ---------------------------------------------------------------------------
# Cada componente (cliente, coordinador, workers) se conecta a AMBOS brokers
# en paralelo. Si uno cae, el otro mantiene el sistema funcionando.

BROKER_PRIMARY_HOST = os.environ.get("BROKER_PRIMARY_HOST", "10.43.97.251")  # David
BROKER_BACKUP_HOST  = os.environ.get("BROKER_BACKUP_HOST",  "10.43.100.92")  # Carol

BROKER_PRIMARY_XSUB_PORT = 5559   # publishers --> broker
BROKER_PRIMARY_XPUB_PORT = 5560   # broker --> subscribers

BROKER_BACKUP_XSUB_PORT = 5561
BROKER_BACKUP_XPUB_PORT = 5562


def broker_pub_endpoints():
    "Endpoints donde los publicadores hacen connect (lado XSUB del proxy)."
    return [
        f"tcp://{BROKER_PRIMARY_HOST}:{BROKER_PRIMARY_XSUB_PORT}",
        f"tcp://{BROKER_BACKUP_HOST}:{BROKER_BACKUP_XSUB_PORT}",
    ]


def broker_sub_endpoints():
    "Endpoints donde los suscriptores hacen connect (lado XPUB del proxy)."
    return [
        f"tcp://{BROKER_PRIMARY_HOST}:{BROKER_PRIMARY_XPUB_PORT}",
        f"tcp://{BROKER_BACKUP_HOST}:{BROKER_BACKUP_XPUB_PORT}",
    ]


# ---------------------------------------------------------------------------
# Estructura de topicos
# ---------------------------------------------------------------------------

TOPIC_REQ = "req.quadratic"                       # Cliente -> Coordinador
TOPIC_RESPONSE_PREFIX = "response."               # Coordinador -> Cliente especifico
TOPIC_TASK_PREFIX = "task."                       # Coordinador -> Worker (task.op1, ...)
TOPIC_RESULT_PREFIX = "result."                   # Worker -> Coordinador (result.<request_id>)
TOPIC_HEARTBEAT_PREFIX = "heartbeat."             # Worker -> Coordinador (heartbeat.op1)
TOPIC_SYSTEM_BROKER_ALIVE = "system.broker.alive" # Broker -> todos


def topic_response(client_id):
    return f"{TOPIC_RESPONSE_PREFIX}{client_id}"


def topic_task(worker_id):
    return f"{TOPIC_TASK_PREFIX}{worker_id}"


def topic_result(request_id):
    return f"{TOPIC_RESULT_PREFIX}{request_id}"


def topic_heartbeat(worker_id):
    return f"{TOPIC_HEARTBEAT_PREFIX}{worker_id}"


# ---------------------------------------------------------------------------
# Workers (operaciones)
# ---------------------------------------------------------------------------
# Plan de roles igual al de los talleres 1 y 2.

ALL_OPS = ["op1", "op2", "op3"]

ROLE_PLAN = {
    "sqrt_discriminant": ["op1", "op2", "op3"],
    "numerator":         ["op2", "op3", "op1"],
    "division":          ["op3", "op1", "op2"],
}


# ---------------------------------------------------------------------------
# Tiempos (segundos)
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 10.0          # cliente: cuanto espera la respuesta total
STAGE_TIMEOUT = 3.0             # coordinador: cuanto espera el resultado de una etapa
HEARTBEAT_INTERVAL = 1.0        # workers: cada cuanto emiten heartbeat
HEARTBEAT_DEAD_AFTER = 4.0      # coordinador: tras cuanto sin heartbeat marca worker como caido
BROKER_HEARTBEAT_INTERVAL = 2.0
SLOW_JOINER_GRACE = 0.8         # pausa tras conectar antes de publicar

EPS = 1e-12
