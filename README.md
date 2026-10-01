# 📊 Estadísticas Service - Pastelería My Dreams

Microservicio backend secundario encargado de gestionar, calcular en tiempo real y exponer las métricas y estadísticas operativas del sistema **Pastelería My Dreams**, conectándose directamente a la base de datos relacional en la nube.

## 📌 Versiones del proyecto

| Rama | Versión | Contenido |
| :--- | :--- | :--- |
| `version-1` | **Entrega 1** | Métricas del catálogo calculadas en vivo desde AWS RDS. |
| `version-2` | **Entrega 2** | Pendiente. |
| `version-3` | **Unidad 3** | Pendiente. |

`main` siempre lleva el último avance del desarrollo.

## 🏢 Equipo de Desarrollo
Diseñado y construido por **8 Digital**.

## 🛠️ Stack Tecnológico
* **Lenguaje:** Java 21
* **Framework:** Spring Boot (LTS) + Spring Web
* **Persistencia / Consultas:** Spring JDBC (`JdbcTemplate`) para consultas dinámicas y eficientes.
* **Base de Datos:** MySQL / Amazon AWS RDS
* **Gestor de dependencias:** Maven
* **Estructura de datos:** JSON

## 🚀 Endpoints Disponibles

| Método HTTP | Ruta | Descripción |
| :--- | :--- | :--- |
| `GET` | `/api/estadisticas` | Consulta y retorna un resumen dinámico con las métricas clave del sistema en tiempo real (total de productos en catálogo, categorías activas, estado operativo y versión). |

## 📈 Statistics from real orders (Kafka)

Since RF-10, `GET /api/estadisticas` also includes statistics derived from **real orders** created in `pedidos-service` and delivered asynchronously through Kafka:

- **Flow:** `pedidos-service` → Kafka topic `pedidos` (configurable via `app.kafka.topic`, default `pedidos`) → `PedidoCreadoListener` in this service → tables → `GET /api/estadisticas`
- **Consumer group:** `estadisticas` (defined in `@KafkaListener`)

### Tables (created automatically at startup with JdbcTemplate DDL)

| Table | Purpose |
| :--- | :--- |
| `pedidos_procesados(pedido_id BIGINT PRIMARY KEY, procesado_en TIMESTAMP)` | Idempotency for Kafka at-least-once redelivery: the listener inserts the order id first; a duplicate key means the event was already counted, so it is skipped (never double-counted). |
| `metricas_pedidos(id INT PRIMARY KEY, pedidos_totales BIGINT, monto_total DECIMAL(12,2), actualizado_en TIMESTAMP)` | Single aggregate row that is upserted on every new order (`pedidos_totales + 1`, `monto_total + total`). |

### Event contract

The listener consumes the exact JSON published by `pedidos-service` (`PedidoCreadoEvent`): `evento`, `id`, `cliente`, `email`, `producto`, `cantidad`, `total`, `fecha`. Malformed or unprocessable messages are logged and skipped (poison-pill tolerance) — the consumer never throws.

### Response fields added

`GET /api/estadisticas` keeps every existing field and adds:

```json
{
  "totalProductosCatalogo": 17,
  "categoriasActivas": 4,
  "estadoServicio": "OPERATIVO",
  "versionSistema": "1.0.0",
  "pedidosTotales": 2,
  "montoTotalPedidos": 48990.00
}
```

`pedidosTotales` and `montoTotalPedidos` default to `0` when no orders have been processed yet or the database is unavailable.

## ⚙️ Configuración y Despliegue Cloud (EC2)
Como parte de la arquitectura cloud, este microservicio está diseñado para ser desplegado en instancias **Amazon EC2**, consumiendo sus endpoints exclusivamente a través de **AWS API Gateway** y apuntando a la base de datos centralizada en **AWS RDS**.

Para probar este microservicio en un entorno de desarrollo local:
1. Asegurarse de tener el JDK 21 instalado.
2. Abrir el proyecto en IntelliJ IDEA.
3. Actualizar las dependencias de Maven.
4. Verificar la configuración de conexión a la base de datos y el puerto (`8081`) en `src/main/resources/application.properties`.
5. Ejecutar la clase principal `EstadisticasServiceApplication.java`.
6. El servicio se inicializará y conectará automáticamente a la base de datos remota para calcular las métricas al instante.

> **Nota de Arquitectura:** Este microservicio forma parte de la arquitectura distribuida del sistema, desacoplando las métricas de control administrativo del núcleo de inventario. Gracias al uso de `JdbcTemplate`, las estadísticas se obtienen de forma dinámica y directa desde la base de datos relacional, asegurando consistencia y alta cohesión modular.