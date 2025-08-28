# Workshop_JPL

# ETL Workshop - Data Engineering Challenge

## 📋 Descripción del Proyecto
Este proyecto implementa un proceso ETL completo para datos de procesos de selección, incluyendo extracción, transformación, carga a un Data Warehouse y generación de KPIs con visualizaciones.

## 🏗️ Arquitectura del Proyecto
- Extract: Carga de datos desde archivo CSV
- Transform: Limpieza, aplicación de reglas de negocio y creación del modelo dimensional
- Load: Carga a Data Warehouse SQLite
- Analysis: Consultas SQL y visualizaciones desde el DW

## 📊 Modelo Dimensional
Esquema estrella con:
- Fact_Hiring: Tabla de hechos con métricas de contratación
- Dim_Date: Dimensión temporal
- Dim_Technology: Dimensión de tecnologías
- Dim_Country: Dimensión geográfica
- Dim_Seniority: Dimensión de niveles de experiencia

## 🚀 Instalación y Uso
1. Clonar el repositorio
2. Instalar dependencias: `pip install -r requirements.txt`
3. Ejecutar el notebook ETL_Notebook.ipynb
4. Las visualizaciones se generarán automáticamente

## 📈 KPIs Implementados
1. Contrataciones por tecnología
2. Contrataciones por año
3. Contrataciones por seniority
4. Contrataciones por país (USA, Brazil, Colombia, Ecuador)
5. Tasa de contratación por tecnología
6. Puntajes promedio por seniority

## 🛠️ Tecnologías Utilizadas
- Python 3.x
- Pandas
- SQLite
- Matplotlib
- Seaborn
- Jupyter Notebook
