"""
Módulo de conexión y gestión de base de datos para ETL Workshop
Optimizado para Visual Studio Code
"""

import mysql.connector
from mysql.connector import Error
import sqlite3
import pandas as pd
import os
from typing import Optional, Union

class DatabaseManager:
    def __init__(self, db_type: str = 'sqlite', **kwargs):
        """
        Inicializa el gestor de base de datos
        
        Args:
            db_type: 'sqlite' o 'mysql'
            **kwargs: Parámetros de conexión
        """
        self.db_type = db_type.lower()
        self.connection = None
        self.config = kwargs
        
        if self.db_type == 'mysql':
            self.host = kwargs.get('host', 'localhost')
            self.user = kwargs.get('user', 'root')
            self.password = kwargs.get('password', '')
            self.database = kwargs.get('database', 'data_warehouse')
        else:
            self.db_path = kwargs.get('db_path', 'data_warehouse.db')
    
    def connect(self) -> Optional[Union[sqlite3.Connection, mysql.connector.MySQLConnection]]:
        """Establece conexión con la base de datos"""
        try:
            if self.db_type == 'mysql':
                return self._connect_mysql()
            else:
                return self._connect_sqlite()
        except Exception as e:
            print(f"❌ Error conectando a {self.db_type.upper()}: {e}")
            return None
    
    def _connect_mysql(self) -> Optional[mysql.connector.MySQLConnection]:
        """Conectar a MySQL"""
        try:
            # Primero conectar sin base de datos para crearla si no existe
            temp_conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            
            cursor = temp_conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.close()
            temp_conn.close()
            
            # Ahora conectar a la base de datos específica
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            
            if connection.is_connected():
                print(f"✅ Conectado a MySQL: {self.database}")
                self.connection = connection
                return connection
                
        except Error as e:
            print(f"❌ Error MySQL: {e}")
            return None
    
    def _connect_sqlite(self) -> Optional[sqlite3.Connection]:
        """Conectar a SQLite"""
        try:
            # Crear directorio si no existe
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            connection = sqlite3.connect(self.db_path)
            print(f"✅ Conectado a SQLite: {self.db_path}")
            self.connection = connection
            return connection
            
        except Error as e:
            print(f"❌ Error SQLite: {e}")
            return None
    
    def create_schema(self) -> bool:
        """Crear el esquema estrella completo"""
        if not self.connection:
            print("❌ No hay conexión establecida")
            return False
        
        try:
            if self.db_type == 'mysql':
                return self._create_mysql_schema()
            else:
                return self._create_sqlite_schema()
        except Exception as e:
            print(f"❌ Error creando esquema: {e}")
            return False
    
    def _create_mysql_schema(self) -> bool:
        """Crear esquema en MySQL"""
        try:
            cursor = self.connection.cursor()
            
            # Tablas dimensionales
            print("📋 Creando tablas dimensionales...")
            
            tables_sql = [
                # Dim_Date
                """
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_id INT AUTO_INCREMENT PRIMARY KEY,
                    application_date DATE NOT NULL UNIQUE,
                    year INT NOT NULL,
                    quarter INT NOT NULL,
                    month INT NOT NULL,
                    day INT NOT NULL,
                    weekday INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                
                # Dim_Technology
                """
                CREATE TABLE IF NOT EXISTS dim_technology (
                    tech_id INT AUTO_INCREMENT PRIMARY KEY,
                    technology VARCHAR(50) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                
                # Dim_Country
                """
                CREATE TABLE IF NOT EXISTS dim_country (
                    country_id INT AUTO_INCREMENT PRIMARY KEY,
                    country VARCHAR(50) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                
                # Dim_Seniority
                """
                CREATE TABLE IF NOT EXISTS dim_seniority (
                    senior_id INT AUTO_INCREMENT PRIMARY KEY,
                    seniority VARCHAR(20) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                
                # Fact_Hiring
                """
                CREATE TABLE IF NOT EXISTS fact_hiring (
                    fact_id INT AUTO_INCREMENT PRIMARY KEY,
                    date_id INT NOT NULL,
                    tech_id INT NOT NULL,
                    country_id INT NOT NULL,
                    senior_id INT NOT NULL,
                    code_challenge_score DECIMAL(4,2) NOT NULL,
                    technical_interview_score DECIMAL(4,2) NOT NULL,
                    yoe DECIMAL(4,1) NOT NULL,
                    is_hired TINYINT(1) NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
                    FOREIGN KEY (tech_id) REFERENCES dim_technology(tech_id),
                    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
                    FOREIGN KEY (senior_id) REFERENCES dim_seniority(senior_id),
                    
                    CONSTRAINT chk_code_score CHECK (code_challenge_score BETWEEN 0 AND 10),
                    CONSTRAINT chk_tech_score CHECK (technical_interview_score BETWEEN 0 AND 10),
                    CONSTRAINT chk_is_hired CHECK (is_hired IN (0, 1))
                )
                """
            ]
            
            for sql in tables_sql:
                cursor.execute(sql)
            
            # Índices
            print("⚡ Creando índices...")
            indexes_sql = [
                "CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_hiring(date_id)",
                "CREATE INDEX IF NOT EXISTS idx_fact_tech ON fact_hiring(tech_id)",
                "CREATE INDEX IF NOT EXISTS idx_fact_country ON fact_hiring(country_id)",
                "CREATE INDEX IF NOT EXISTS idx_fact_senior ON fact_hiring(senior_id)",
                "CREATE INDEX IF NOT EXISTS idx_fact_hired ON fact_hiring(is_hired)",
                "CREATE INDEX IF NOT EXISTS idx_date_date ON dim_date(application_date)",
                "CREATE INDEX IF NOT EXISTS idx_tech_name ON dim_technology(technology)",
                "CREATE INDEX IF NOT EXISTS idx_country_name ON dim_country(country)",
                "CREATE INDEX IF NOT EXISTS idx_seniority_name ON dim_seniority(seniority)"
            ]
            
            for sql in indexes_sql:
                cursor.execute(sql)
            
            self.connection.commit()
            cursor.close()
            
            print("✅ Esquema estrella creado en MySQL")
            return True
            
        except Error as e:
            print(f"❌ Error creando esquema MySQL: {e}")
            return False
    
    def _create_sqlite_schema(self) -> bool:
        """Crear esquema en SQLite"""
        try:
            cursor = self.connection.cursor()
            
            # Eliminar tablas existentes (para desarrollo)
            print("🧹 Limpiando tablas existentes...")
            tables = ['fact_hiring', 'dim_date', 'dim_technology', 'dim_country', 'dim_seniority']
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            
            # Crear tablas
            print("📋 Creando tablas dimensionales...")
            
            tables_sql = [
                # Dim_Date
                """
                CREATE TABLE dim_date (
                    date_id INTEGER PRIMARY KEY,
                    application_date DATE NOT NULL UNIQUE,
                    year INTEGER NOT NULL,
                    quarter INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    day INTEGER NOT NULL,
                    weekday INTEGER NOT NULL
                )
                """,
                
                # Dim_Technology
                """
                CREATE TABLE dim_technology (
                    tech_id INTEGER PRIMARY KEY,
                    technology TEXT NOT NULL UNIQUE
                )
                """,
                
                # Dim_Country
                """
                CREATE TABLE dim_country (
                    country_id INTEGER PRIMARY KEY,
                    country TEXT NOT NULL UNIQUE
                )
                """,
                
                # Dim_Seniority
                """
                CREATE TABLE dim_seniority (
                    senior_id INTEGER PRIMARY KEY,
                    seniority TEXT NOT NULL UNIQUE
                )
                """,
                
                # Fact_Hiring
                """
                CREATE TABLE fact_hiring (
                    fact_id INTEGER PRIMARY KEY,
                    date_id INTEGER NOT NULL,
                    tech_id INTEGER NOT NULL,
                    country_id INTEGER NOT NULL,
                    senior_id INTEGER NOT NULL,
                    code_challenge_score REAL NOT NULL,
                    technical_interview_score REAL NOT NULL,
                    yoe REAL NOT NULL,
                    is_hired INTEGER NOT NULL CHECK (is_hired IN (0, 1)),
                    FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
                    FOREIGN KEY (tech_id) REFERENCES dim_technology (tech_id),
                    FOREIGN KEY (country_id) REFERENCES dim_country (country_id),
                    FOREIGN KEY (senior_id) REFERENCES dim_seniority (senior_id)
                )
                """
            ]
            
            for sql in tables_sql:
                cursor.execute(sql)
            
            # Índices
            print("⚡ Creando índices...")
            indexes_sql = [
                "CREATE INDEX idx_fact_date ON fact_hiring(date_id)",
                "CREATE INDEX idx_fact_tech ON fact_hiring(tech_id)",
                "CREATE INDEX idx_fact_country ON fact_hiring(country_id)",
                "CREATE INDEX idx_fact_senior ON fact_hiring(senior_id)",
                "CREATE INDEX idx_fact_hired ON fact_hiring(is_hired)",
                "CREATE INDEX idx_date_date ON dim_date(application_date)"
            ]
            
            for sql in indexes_sql:
                cursor.execute(sql)
            
            self.connection.commit()
            
            print("✅ Esquema estrella creado en SQLite")
            return True
            
        except Error as e:
            print(f"❌ Error creando esquema SQLite: {e}")
            return False
    
    def execute_query(self, query: str, params: tuple = None) -> Optional[list]:
        """Ejecutar consulta SQL"""
        try:
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return result, columns
            else:
                self.connection.commit()
                return cursor.rowcount
            
        except Error as e:
            print(f"❌ Error ejecutando query: {e}")
            return None
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def get_table_info(self) -> None:
        """Mostrar información de las tablas"""
        if not self.connection:
            print("❌ No hay conexión establecida")
            return
        
        try:
            if self.db_type == 'mysql':
                cursor = self.connection.cursor()
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]
                
                print("\n📊 TABLAS EN LA BASE DE DATOS:")
                print("=" * 50)
                
                for table in tables:
                    print(f"\n📋 {table}:")
                    cursor.execute(f"DESCRIBE {table}")
                    for column in cursor.fetchall():
                        print(f"   {column[0]:<20} {column[1]:<15} {column[2]}")
                
                cursor.close()
                
            else:  # SQLite
                cursor = self.connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [table[0] for table in cursor.fetchall()]
                
                print("\n📊 TABLAS EN LA BASE DE DATOS:")
                print("=" * 50)
                
                for table in tables:
                    print(f"\n📋 {table}:")
                    cursor.execute(f"PRAGMA table_info({table})")
                    for column in cursor.fetchall():
                        print(f"   {column[1]:<20} {column[2]:<15} {'PK' if column[5] > 0 else ''}")
                
                cursor.close()
                
        except Error as e:
            print(f"❌ Error obteniendo información: {e}")
    
    def close(self) -> None:
        """Cerrar conexión"""
        if self.connection:
            if self.db_type == 'mysql' and self.connection.is_connected():
                self.connection.close()
                print("✅ Conexión MySQL cerrada")
            else:
                self.connection.close()
                print("✅ Conexión SQLite cerrada")

# Función de utilidad para configuración rápida
def setup_database(db_type: str = 'sqlite', **kwargs) -> DatabaseManager:
    """
    Configuración rápida de la base de datos
    
    Args:
        db_type: 'sqlite' o 'mysql'
        **kwargs: Parámetros de conexión
    
    Returns:
        DatabaseManager instance
    """
    print("=" * 60)
    print("🛠️  CONFIGURANDO BASE DE DATOS ETL")
    print("=" * 60)
    
    db_manager = DatabaseManager(db_type, **kwargs)
    connection = db_manager.connect()
    
    if connection:
        success = db_manager.create_schema()
        if success:
            db_manager.get_table_info()
        else:
            print("❌ Error creando el esquema")
    else:
        print("❌ No se pudo establecer conexión")
    
    db_manager.close()
    
    print("=" * 60)
    print("🎉 CONFIGURACIÓN COMPLETADA")
    print("=" * 60)
    
    return db_manager

# Ejemplo de uso
if __name__ == "__main__":
    # Configurar SQLite (recomendado para desarrollo)
    print("🚀 Configurando SQLite...")
    sqlite_manager = setup_database(
        db_type='sqlite',
        db_path='data/data_warehouse.db'
    )
    
    # Opcional: Configurar MySQL si está disponible
    try:
        print("\n🚀 Intentando configurar MySQL...")
        mysql_manager = setup_database(
            db_type='mysql',
            host='localhost',
            user='root',
            password='',  # Tu password aquí
            database='data_warehouse'
        )
    except Exception as e:
        print(f"⚠️  MySQL no disponible: {e}")