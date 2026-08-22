# 📊 Estadísticas Service - Pastelería My Dreams

Microservicio backend secundario encargado de gestionar y exponer las métricas y estadísticas operativas del sistema **Pastelería My Dreams**.

## 🏢 Equipo de Desarrollo
Diseñado y construido por **8 Digital**.

## 🛠️ Stack Tecnológico
* **Lenguaje:** Java 21
* **Framework:** Spring Boot (LTS) + Spring Web
* **Gestor de dependencias:** Maven
* **Estructura de datos:** JSON

## 🚀 Endpoints Disponibles

| Método HTTP | Ruta | Descripción |
| :--- | :--- | :--- |
| `GET` | `/api/estadisticas` | Retorna un resumen con las métricas clave del sistema (total de productos en catálogo, categorías activas, estado operativo y versión). |

## ⚙️ Configuración y Despliegue Cloud (EC2)
Como parte de la arquitectura cloud, este microservicio está diseñado para ser desplegado en instancias **Amazon EC2**, consumiendo sus endpoints exclusivamente a través de **AWS API Gateway**.

Para probar este microservicio en un entorno de desarrollo:
1. Asegurarse de tener el JDK 21 instalado.
2. Abrir el proyecto en IntelliJ IDEA.
3. Actualizar las dependencias de Maven.
4. Verificar la configuración del puerto en `src/main/resources/application.properties`.
5. Ejecutar la clase principal `EstadisticasServiceApplication.java`.
6. El servidor se inicializará por defecto en el puerto `8081`.

> **Nota de Arquitectura:** Este microservicio forma parte de la arquitectura distribuida del sistema, desacoplando las métricas de control administrativo del núcleo de inventario para asegurar una alta cohesión y modularidad.