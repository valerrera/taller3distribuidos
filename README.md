# Taller 3 — Patrón Publicador/Suscriptor con ZeroMQ

Sistema distribuido para resolver la ecuación cuadrática (misma lógica que
los Talleres 1 y 2) sustituyendo el middleware:

| Taller | Middleware                       |
| ------ | -------------------------------- |
| T1     | Sockets TCP + JSON               |
| T2     | gRPC + Protocol Buffers          |
| **T3** | **ZeroMQ Pub/Sub + Broker**      |

---

## Asignación de máquinas del grupo

| Rol           | Quien    | IP             |
| ------------- | -------- | -------------- |
| Worker 1      | Valeria  | 10.43.99.136   |
| Worker 2      | Xiomara  | 10.43.99.139   |
| Worker 3      | Alviz    | 10.43.97.155   |
| Coordinador   | David    | 10.43.97.251   |
| Cliente       | Carol    | 10.43.100.92   |

Los **dos brokers** se ubican en las máquinas con menos carga de cálculo:

| Broker        | Va en la máquina de | IP             | Puertos       |
| ------------- | ------------------- | -------------- | ------------- |
| Broker PRIMARIO | David (Coordinador) | 10.43.97.251   | XSUB 5559 / XPUB 5560 |
| Broker BACKUP   | Carol (Cliente)     | 10.43.100.92   | XSUB 5561 / XPUB 5562 |

> Cada componente (cliente, coordinador, workers, monitor) **se conecta
> a los dos brokers en paralelo**. Si uno cae, el otro mantiene el
> sistema funcionando. Los receptores deduplican por `msg_id` para
> no procesar el mismo mensaje dos veces.

---

## 1. Instalación (en TODAS las máquinas)

```bash
# clonar el repo
git clone https://github.com/valerrera/taller3distribuidos.git
cd taller3distribuidos

# instalar pyzmq (es la unica dependencia)
pip3 install -r requirements.txt
```

Si `pip3` no está, usar:
```bash
python3 -m pip install -r requirements.txt
```

---

## 2. Orden de arranque (paso a paso)

> **IMPORTANTE**: arrancar en este orden exacto, esperando ~2 segundos
> entre máquinas. Cada componente abre sus sockets contra los dos brokers,
> así que los brokers deben estar antes que todo lo demás.

### Paso 1 — David enciende el Broker PRIMARIO

En la máquina de **David (10.43.97.251)**:

```bash
python3 broker.py primary
```

Debe imprimirse:
```
[BROKER:primary] BROKER PRIMARY arrancando
[BROKER:primary] escuchando publicadores en  tcp://*:5559  (XSUB)
[BROKER:primary] sirviendo suscriptores en  tcp://*:5560  (XPUB)
```

### Paso 2 — Carol enciende el Broker BACKUP

En la máquina de **Carol (10.43.100.92)**:

```bash
python3 broker.py backup
```

Debe imprimirse:
```
[BROKER:backup] BROKER BACKUP arrancando
[BROKER:backup] escuchando publicadores en  tcp://*:5561  (XSUB)
[BROKER:backup] sirviendo suscriptores en  tcp://*:5562  (XPUB)
```

### Paso 3 — David enciende el Coordinador

En la **misma máquina de David**, en **otra terminal**:

```bash
python3 coordinator.py
```

Debe imprimirse:
```
[COORD] COORDINADOR arrancando
[COORD] suscrito a topico de solicitudes: req.quadratic
[COORD] esperando solicitudes...
```

### Paso 4 — Valeria, Xiomara y Alviz encienden sus workers

**En paralelo** (no importa el orden entre ellos):

| Persona | Máquina       | Comando                   |
| ------- | ------------- | ------------------------- |
| Valeria | 10.43.99.136  | `python3 worker.py op1`   |
| Xiomara | 10.43.99.139  | `python3 worker.py op2`   |
| Alviz   | 10.43.97.155  | `python3 worker.py op3`   |

Cada worker debe imprimir:
```
[OPx] WORKER opx arrancando
[OPx] suscrito a topico: task.opx
[OPx] esperando tareas...
[OPx] latido #0 -> heartbeat.opx
```

A los pocos segundos, en la terminal del **Coordinador** debe aparecer:
```
[COORD] @@@ transicion: op1 ahora esta VIVO (heartbeat detectado)
[COORD] @@@ transicion: op2 ahora esta VIVO (heartbeat detectado)
[COORD] @@@ transicion: op3 ahora esta VIVO (heartbeat detectado)
```

### Paso 5 — Carol arranca el Cliente

En la **misma máquina de Carol**, en **otra terminal**:

```bash
# modo interactivo (pide a, b, c)
python3 client.py

# o one-shot
python3 client.py 1 -3 2
```

Salida del cliente (lo único que ve el usuario final):
```
=== Cliente cuadratico (client-abc123) ===
>>> Solicitud: resolver  1.0x^2 + (-3.0)x + 2.0 = 0
<<< Respuesta:
      x1 = 2.0
      x2 = 1.0
```

> El cliente **no** muestra detalles internos (workers caídos, broker
> caído, modo de respuesta, traza, reintentos…). Toda esa información
> aparece en las terminales del coordinador y los workers.

### Paso 6 (opcional) — Monitor del bus

Para sustentar visualmente la actividad del bus pub/sub, en cualquier
máquina (con la dependencia ya instalada):

```bash
python3 monitor.py
```

Imprime CADA mensaje que pasa por los topicos del sistema, etiquetado:
```
[MONITOR] [req-cliente]   req.quadratic  {'request_id': '...', 'a': 1.0, ...}
[MONITOR] [task->worker]  task.op1       {'a': 1.0, 'b': -3.0, ...}
[MONITOR] [result->coord] result.<id>    {'sqrt_d': 1.0, ...}
[MONITOR] [resp->cliente] response.<id>  {'x1': 2.0, 'x2': 1.0, ...}
```

---

## 3. Pruebas para la sustentación

| Escenario                       | Cómo provocarlo                                  | Qué se debe ver                                                                       |
| ------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------- |
| Sin fallos                      | Todos arriba                                     | Cliente: x1, x2. Coordinador: traza `sqrt:op1 -> num:op2 -> div:op3`                   |
| Cae 1 worker                    | `Ctrl+C` en `worker.py op1`                      | Cliente sigue recibiendo respuesta. Coordinador: log `transicion: op1 ahora esta CAIDO`, traza usa op2/op3 |
| Caen 2 workers                  | `Ctrl+C` en op1 y op2                            | Cliente sigue recibiendo. Coordinador: `modo SINGLE_NODE_FALLBACK`, traza `full_quadratic:op3` |
| Caen los 3 workers              | `Ctrl+C` en op1, op2, op3                        | Cliente: error genérico. Coordinador: `no hay nodos disponibles`                       |
| Cae broker primario             | `Ctrl+C` en `broker.py primary`                  | Cliente: respuestas siguen llegando vía backup. Coordinador y workers siguen logueando |
| Vuelve broker primario          | `python3 broker.py primary` otra vez             | ZMQ reconecta automáticamente (ver logs `+SUB` en el primario nuevo)                   |
| Cliente nuevo                   | `python3 client.py --id otro 4 -4 1`             | El cliente nuevo recibe su respuesta sin que nadie reconfigure nada                    |
| Worker nuevo en caliente        | `python3 worker.py op4` en cualquier máquina     | El monitor ve sus heartbeats; para que el coordinador lo use hay que añadirlo a `config.ROLE_PLAN` |

---

## 4. Estructura de tópicos

| Tópico                        | Dirección                | Contenido                                |
| ----------------------------- | ------------------------ | ---------------------------------------- |
| `req.quadratic`               | Cliente → Coordinador    | `{request_id, client_id, a, b, c}`       |
| `task.<op>` (op1/op2/op3/...) | Coordinador → Worker     | `{request_id, stage, a, b, c, ...}`      |
| `result.<request_id>`         | Worker → Coordinador     | `{request_id, stage, worker, ok, ...}`   |
| `response.<client_id>`        | Coordinador → Cliente    | `{request_id, ok, x1, x2, mode, trace}`  |
| `heartbeat.<op>`              | Worker → todos           | `{worker_id, ts, tick}`                  |
| `system.broker.alive`         | Broker → todos           | `{broker_id, ts, tick}`                  |

---

## 5. Componentes / archivos

| Archivo          | Rol                                                              |
| ---------------- | ---------------------------------------------------------------- |
| `broker.py`      | Proxy XSUB/XPUB en nodo separado, con logging verbose            |
| `coordinator.py` | Orquestador del pipeline (failover por roles + single_node)      |
| `worker.py`      | Worker (op1/op2/op3/...): ejecuta una etapa o el cálculo full    |
| `client.py`      | CLI minimalista — solo solicitud y respuesta                     |
| `monitor.py`     | Suscriptor a todos los tópicos (visualización en vivo)           |
| `messaging.py`   | Helpers `Publisher`/`Subscriber` con dual-broker + dedupe        |
| `log.py`         | Helper de logging con timestamp                                   |
| `config.py`      | IPs, puertos, tópicos, role-plan, timeouts                       |
| `diagramas.md`   | Diagramas de arquitectura y secuencia (Mermaid) para el informe   |
| `requirements.txt` | `pyzmq>=25.0.0`                                                |
