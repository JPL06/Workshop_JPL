from sqlalchemy import create_engine

user = 'root'         # Cambia por tu usuario
password = 'root'     # Cambia por tu contraseña
host = 'localhost'
port = 3306
database = 'jpl_work'  # Cambia por tu base de datos

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')