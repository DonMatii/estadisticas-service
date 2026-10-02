# 📊 Estadísticas Service - Pastelería My Dreams

Microservicio backend secundario encargado de gestionar, calcular en tiempo real y exponer las métricas y estadísticas operativas del sistema **Pastelería My Dreams**, conectándose directamente a la base de datos relacional y consumiendo los pedidos reales desde Kafka.

## 📌 Versiones del proyecto

| Rama | Versión | Contenido |
| :--- | :--- | :--- |
| `version-1` | **Entrega 1** | Métricas del catálogo calculadas en vivo desde AWS RDS. |
| `version-2` | **Entrega 2** | Estadísticas de pedidos reales recibidas por Kafka (RF-10). |
| `version-3` | **Unidad 3** | Pendiente. |

`main` siempre lleva el último avance del desarrollo.

## 🏢 Equipo de Desarrollo
Diseñado y construido por **8 Digital**.

## 🛠️ Stack Tecnológico
* **Lenguaje:** Java 21
* **Framework:** Spring Boot 3.2.5 + Spring Web
* **Persistencia / Consultas:** Spring JDBC (`JdbcTemplate`) para consultas dinámicas y eficientes.
* **Mensajería:** Apache Kafka (consumidor, topic `pedidos`)
* **Base de Datos:** MySQL local / Amazon AWS RDS
* **Gestor de dependencias:** Maven
* **Estructura de datos:** JSON

## 🚀 Endpoints Disponibles

| Método HTTP | Ruta | Descripción |
| :--- | :--- | :--- |
| `GET` | `/api/estadisticas` | Consulta y retorna un resumen con las métricas clave del sistema: catálogo, estado del servicio, versión y estadísticas de pedidos reales recibidos por Kafka. |

### Respuesta

Ejemplo real de la verificación E2E (2 pedidos procesados):

```json
{
  "pedidosTotales": 2,
  "estadoServicio": "OPERATIVO",
  "versionSistema": "1.0.0",
  "categoriasActivas": 1,
  "totalProductosCatalogo": 1,
  "montoTotalPedidos": 40000.00
}
```

| Campo | Origen |
| :--- | :--- |
| `totalProductosCatalogo` | `COUNT(*)` sobre la tabla `productos`. |
| `categoriasActivas` | `COUNT(DISTINCT categoria)` sobre la tabla `productos`. |
| `pedidosTotales` | Fila única de la tabla `metricas_pedidos` (alimentada por Kafka, RF-10). |
| `montoTotalPedidos` | Suma de los montos de los pedidos procesados (RF-10). |
| `estadoServicio` | Siempre `OPERATIVO` cuando el servicio responde. |
| `versionSistema` | Versión declarada del servicio (`1.0.0`). |

Si la base de datos no está disponible, la respuesta sigue completa: `pedidosTotales` y `montoTotalPedidos` vuelven `0`, y el catálogo usa valores de respaldo.

## 📈 Estadísticas de pedidos reales (Kafka, RF-10)

Desde la Entrega 2, `GET /api/estadisticas` incluye estadísticas derivadas de los **pedidos reales** creados en `pedidos-service` y entregados de forma asíncrona por Kafka:

- **Flujo:** `pedidos-service` → topic `pedidos` (configurable con `app.kafka.topic`, default `pedidos`) → `PedidoCreadoListener` de este servicio → tablas → `GET /api/estadisticas`
- **Consumer group:** `estadisticas` (definido en `@KafkaListener`)

### Tablas (creadas automáticamente al arrancar con DDL vía `JdbcTemplate`)

| Tabla | Propósito |
| :--- | :--- |
| `pedidos_procesados(pedido_id BIGINT PRIMARY KEY, procesado_en TIMESTAMP)` | Idempotencia para la reentrega at-least-once de Kafka: el listener inserta primero el id del pedido; una clave duplicada significa que el evento ya fue contado, así que se omite (nunca se cuenta dos veces). |
| `metricas_pedidos(id INT PRIMARY KEY, pedidos_totales BIGINT, monto_total DECIMAL(12,2), actualizado_en TIMESTAMP)` | Fila única de agregados que se actualiza (`upsert`) con cada pedido nuevo (`pedidos_totales + 1`, `monto_total + total`). |

Ambas se crean con `CREATE TABLE IF NOT EXISTS` en un `@PostConstruct`; si la base no está disponible, el servicio solo avisa en el log y sigue vivo.

### Contrato del evento

El listener consume el JSON exacto que publica `pedidos-service` (`PedidoCreadoEvent`): `evento`, `id`, `cliente`, `email`, `producto`, `cantidad`, `total`, `fecha`. Mensajes malformados o no procesables se registran y se saltan (tolerancia a poison pill) — el consumidor nunca lanza excepción.

### Evidencia E2E

Con 2 pedidos registrados, el endpoint evolucionó de `1 / 30000.00` a `2 / 40000.00`, sin dobles conteos.

## 🔄 Flujo end-to-end

```
Frontend (formulario)
   │  POST /api/pedidos
   ▼
pedidos-service (8082)
   │  persiste el pedido y publica PedidoCreado
   ▼
Kafka topic `pedidos`
   ├──▶ notificaciones-service (8083)  → persiste la notificación (estado PENDIENTE)
   └──▶ estadisticas-service  (8081)   → actualiza pedidosTotales / montoTotalPedidos
```

## 🐳 Kafka local (broker)

El broker vive en el repositorio de `pedidos-service` (no hay compose en este repo):

```powershell
docker start pasteleria-kafka
# o, si el contenedor no existe todavía, desde el repo pedidos-service:
docker compose up -d
```

| Setting | Valor |
| :--- | :--- |
| Topic | `pedidos` (compartido con `pedidos-service`) |
| Consumer group | `estadisticas` |
| Broker | `localhost:9092` |

## ⚙️ Variables de entorno

La conexión a la base de datos se configura por variables de entorno (RNF-11; nunca se commitean secretos), con defaults locales:

| Variable | Default local |
| :--- | :--- |
| `DB_URL` | `jdbc:mysql://localhost:3306/pasteleria_my_dreams?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC` |
| `DB_USER` | `root` |
| `DB_PASS` | *(vacío)* |

Otras propiedades relevantes (`src/main/resources/application.properties`): `server.port=8081`, `spring.kafka.bootstrap-servers=localhost:9092`, `app.kafka.topic=pedidos`.

## ▶️ Cómo correrlo local

Requisitos: JDK 21, MySQL 8 local con la base `pasteleria_my_dreams` y el broker Kafka arriba:

```powershell
docker start pasteleria-kafka
.\mvnw.cmd spring-boot:run   # servicio en el puerto 8081
```

Alternativa desde IntelliJ IDEA: abrir el proyecto, actualizar las dependencias de Maven y ejecutar la clase principal `EstadisticasServiceApplication.java`.

## 🧪 Tests

```powershell
.\mvnw.cmd test
```

Los tests son herméticos: usan H2 en memoria y un broker embebido (`@EmbeddedKafka`), por lo que no requieren MySQL ni contenedores de Docker en marcha.

## ⚙️ Configuración y Despliegue Cloud (EC2)
Como parte de la arquitectura cloud, este microservicio está diseñado para ser desplegado en instancias **Amazon EC2**, consumiendo sus endpoints exclusivamente a través de **AWS API Gateway** y apuntando a la base de datos centralizada en **AWS RDS**.

> **Nota de Arquitectura:** Este microservicio forma parte de la arquitectura distribuida del sistema, desacoplando las métricas de control administrativo del núcleo de inventario. Gracias al uso de `JdbcTemplate`, las estadísticas se obtienen de forma dinámica y directa desde la base de datos relacional, asegurando consistencia y alta cohesión modular.
