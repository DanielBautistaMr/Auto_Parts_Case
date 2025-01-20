# Auto Parts Case
![preview-removebg-preview](https://github.com/user-attachments/assets/e43ab55d-9f75-4ee4-90b4-061506c0df29)

Este proyecto, titulado **"Auto Parts Case"**, fue desarrollado como parte de mi preparación para una entrevista técnica para el puesto de Consultor de Datos Builder Junior en Information Workers S.A.S. El objetivo principal es demostrar habilidades en el manejo de datos, integración de diversas fuentes de información y análisis interactivo, resolviendo una problemática clave identificada en una empresa de autopartes.

## Problema Identificado

La empresa enfrenta dificultades para controlar y gestionar los datos provenientes de diversas fuentes. Esto incluye:

- Falta de un sistema centralizado para almacenar y consultar los datos.
- Ineficiencias en la toma de decisiones debido a información desorganizada.
- Dificultad para analizar tendencias y patrones en el inventario y las ventas.

El sistema propuesto busca abordar estas dificultades mediante una solución robusta y escalable.

## Descripción del Proyecto

El sistema desarrollado aborda las siguientes funcionalidades clave:

- **Gestor de datos centralizado**: Permite registrar, actualizar y consultar información de diversas fuentes en un único lugar.
- **Gráficos interactivos**: Ofrecen visualizaciones claras y dinámicas de los datos de inventario, ventas y tendencias.
- **Consultas en tiempo real**: Integración de una API basada en inteligencia artificial que permite realizar consultas dinámicas sobre los datos, tales como "Listar ventas de los últimos 5 días".

Este proyecto está diseñado con un enfoque escalable, garantizando que pueda adaptarse a futuros cambios o expansiones.

## Tecnologías Utilizadas

El desarrollo del proyecto incluye las siguientes herramientas y tecnologías:

- **Lenguajes y Frameworks**: Python, Flask/FastAPI para el backend.
- **Base de Datos**: MongoDB, por su capacidad de manejo de datos no relacionales.
- **Visualizaciones**: Matplotlib/Plotly para gráficos interactivos.
- **Modelo de IA**: ChatGPT basado en Hugging Face para consultas dinámicas.
- **APIs**: Uso de la Fake Store API para simular datos realistas.
- **Entorno de Desarrollo**: Docker para garantizar portabilidad y consistencia.

## Características Principales

1. **Extracción y Transformación de Datos**:
   - Se implementó un proceso ETL (Extracción, Transformación y Carga) para consumir datos desde la Fake Store API, limpiarlos y almacenarlos en MongoDB.

2. **API Dinámica**:
   - Permite realizar consultas como:
     - "¿Cuántos productos están disponibles en X categoría?"
     - "Listar las ventas de los últimos 5 días."
     - "Mostrar el producto más vendido."

3. **Visualizaciones Gráficas**:
   - Dashboards para visualizar datos clave del inventario y las ventas.

4. **Escalabilidad y Adaptabilidad**:
   - Diseñado para ser fácilmente ampliable con nuevos módulos y funcionalidades.

## Instalación

Para ejecutar este proyecto en tu máquina local, sigue estos pasos:

1. Clona el repositorio:
   ```bash
   git clone git@github.com:DanielBautistaMr/Auto_Parts_Case.git
   cd Auto_Parts_Case
   ```

2. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

3. Configura las variables de entorno:
   - Crea un archivo `.env` basado en `.env.example`.
   - Agrega tus configuraciones, como las credenciales de la base de datos y el token para la Fake Store API.

4. Ejecuta la aplicación:
   ```bash
   python main.py
   ```

5. Accede a la aplicación en tu navegador:
   - API: `http://localhost:5000`

## Uso

- Para interactuar con el sistema, puedes usar herramientas como Postman o tu navegador para realizar consultas a la API.

## Ejemplos de Consultas Dinámicas

- **Productos disponibles**: `GET /api/products?category=X`
- **Ventas recientes**: `GET /api/sales?days=5`
- **Producto más vendido**: `GET /api/products/top-seller`

## Contribuciones

Este proyecto fue desarrollado individualmente como parte de mi preparación técnica, pero cualquier sugerencia o retroalimentación es bienvenida.

## Licencia

Este proyecto está bajo una licencia de uso personal para fines educativos.

---

Si tienes alguna pregunta o sugerencia, no dudes en contactarme a través de mi [perfil de GitHub](https://github.com/DanielBautistaMr).

