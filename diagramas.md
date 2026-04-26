# Diagramas para el informe — Taller 3 (Pub/Sub con ZeroMQ)

> Los siguientes diagramas están escritos en sintaxis **Mermaid**, que se
> renderiza directamente en GitHub o en cualquier visor compatible
> (VS Code, Mermaid Live Editor: https://mermaid.live).

---

## 1. Diagrama de arquitectura

```mermaid
flowchart LR
    subgraph Brokers["Nodos de Broker (separados)"]
        BP["Broker PRIMARIO<br/>XSUB :5559<br/>XPUB :5560"]
        BB["Broker BACKUP<br/>XSUB :5561<br/>XPUB :5562"]
    end

    subgraph Clientes
        C1[Cliente 1]
        C2[Cliente 2]
    end

    COORD[Coordinador]

    subgraph Workers
        OP1[Worker op1]
        OP2[Worker op2]
        OP3[Worker op3]
    end

    MON[Monitor]

    C1 -- "PUB req.quadratic" --> BP
    C1 -- "PUB req.quadratic" --> BB
    C2 -- "PUB req.quadratic" --> BP
    C2 -- "PUB req.quadratic" --> BB

    BP -- "SUB response.client_id" --> C1
    BB -- "SUB response.client_id" --> C1
    BP -- "SUB response.client_id" --> C2
    BB -- "SUB response.client_id" --> C2

    BP <-- "SUB req.* / heartbeat.* / result.*<br/>PUB task.* / response.*" --> COORD
    BB <-- "SUB req.* / heartbeat.* / result.*<br/>PUB task.* / response.*" --> COORD

    BP <-- "SUB task.opN<br/>PUB result.* / heartbeat.opN" --> OP1
    BB <-- "SUB task.opN<br/>PUB result.* / heartbeat.opN" --> OP1
    BP <-- "..." --> OP2
    BB <-- "..." --> OP2
    BP <-- "..." --> OP3
    BB <-- "..." --> OP3

    BP -- "SUB *" --> MON
    BB -- "SUB *" --> MON
```

**Notas:**
- Cada componente (cliente, coordinador, worker, monitor) abre **un solo socket
  PUB y un solo socket SUB** que hacen `connect` a **los dos brokers a la vez**.
- ZMQ permite multi-connect: si uno de los dos brokers cae, el socket sigue
  entregando por el otro de forma transparente.
- Los receptores deduplican por `msg_id` para que la doble entrega no se
  procese dos veces.

---

## 2. Diagrama de secuencia — flujo normal (sin fallos)

```mermaid
sequenceDiagram
    autonumber
    participant Cli as Cliente
    participant B as Broker (primario+backup)
    participant Co as Coordinador
    participant O1 as Op1
    participant O2 as Op2
    participant O3 as Op3

    Note over O1,O3: Cada worker emite heartbeat.<op> cada 1 s

    Cli->>B: PUB req.quadratic {req_id, client_id, a, b, c}
    B->>Co: SUB req.quadratic
    Co->>B: PUB task.op1 {req_id, stage=sqrt_discriminant, a,b,c}
    B->>O1: SUB task.op1
    O1->>B: PUB result.{req_id} {stage=sqrt_discriminant, sqrt_d, ok=true}
    B->>Co: SUB result.{req_id}

    Co->>B: PUB task.op2 {req_id, stage=numerator, b, sqrt_d}
    B->>O2: SUB task.op2
    O2->>B: PUB result.{req_id} {stage=numerator, num_plus, num_minus}
    B->>Co: SUB result.{req_id}

    Co->>B: PUB task.op3 {req_id, stage=division, a, num_plus, num_minus}
    B->>O3: SUB task.op3
    O3->>B: PUB result.{req_id} {stage=division, x1, x2}
    B->>Co: SUB result.{req_id}

    Co->>B: PUB response.{client_id} {ok=true, x1, x2, mode="pipeline"}
    B->>Cli: SUB response.{client_id}
```

---

## 3. Diagrama de secuencia — fallo de un worker (failover de etapa)

```mermaid
sequenceDiagram
    autonumber
    participant Cli as Cliente
    participant B as Broker
    participant Co as Coordinador
    participant O1 as Op1 (caído)
    participant O2 as Op2
    participant O3 as Op3

    Note over O1: Op1 deja de emitir heartbeat
    Note over Co: heartbeat.op1 expira > 4 s ⇒ dead_ops += op1

    Cli->>B: PUB req.quadratic {a,b,c}
    B->>Co: req
    Co->>B: PUB task.op1 (stage=sqrt_discriminant)
    Note over Co: timeout 3 s sin result.{req_id}<br/>dead_ops += op1
    Co->>B: PUB task.op2 (stage=sqrt_discriminant)  [sustituto]
    O2->>B: PUB result.{req_id} (sqrt_d)

    Co->>B: PUB task.op2 (stage=numerator)  [op2 sigue siendo principal de numerator]
    O2->>B: PUB result.{req_id} (num_plus, num_minus)

    Co->>B: PUB task.op3 (stage=division)  [op3 principal]
    O3->>B: PUB result.{req_id} (x1, x2)

    Co->>B: PUB response.{client_id} {mode="pipeline", dead_ops=["op1"]}
    B->>Cli: response
```

---

## 4. Diagrama de secuencia — dos workers caídos (single_node_fallback)

```mermaid
sequenceDiagram
    autonumber
    participant Cli as Cliente
    participant B as Broker
    participant Co as Coordinador
    participant O3 as Op3 (único vivo)

    Note over Co: heartbeat.op1 y heartbeat.op2 expiran<br/>dead_ops = {op1, op2}

    Cli->>B: PUB req.quadratic
    B->>Co: req
    Note over Co: len(dead_ops) >= 2 ⇒ activar single_node_fallback
    Co->>B: PUB task.op3 {stage=full_quadratic, a,b,c}
    O3->>B: PUB result.{req_id} {x1, x2}
    Co->>B: PUB response.{client_id} {mode="single_node_fallback", trace="full_quadratic:op3"}
    B->>Cli: response
```

---

## 5. Diagrama de secuencia — caída del broker primario

```mermaid
sequenceDiagram
    autonumber
    participant Cli as Cliente
    participant BP as Broker PRIMARIO
    participant BB as Broker BACKUP
    participant Co as Coordinador

    Cli->>BP: PUB req.quadratic
    Cli->>BB: PUB req.quadratic (mismo mensaje, multi-connect)

    Note over BP: BROKER PRIMARIO SE CAE
    BB->>Co: req.quadratic  (entregado por backup)

    Co->>BB: PUB task.opN
    Note over Co: Cuando vuelva a estar arriba, ZMQ<br/>reconecta automáticamente al primario
```

> Como cada PUB hace connect a los dos brokers y cada SUB también, la caída
> de uno de ellos es invisible: el otro sigue entregando. La deduplicación
> por `msg_id` impide que el mismo mensaje sea procesado dos veces cuando
> ambos brokers están vivos.

---

## 6. Tabla de roles (igual que Talleres 1 y 2)

| Etapa                | Principal | Sustituto 1 | Sustituto 2 |
| -------------------- | --------- | ----------- | ----------- |
| sqrt_discriminant    | op1       | op2         | op3         |
| numerator            | op2       | op3         | op1         |
| division             | op3       | op1         | op2         |
