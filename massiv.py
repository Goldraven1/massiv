import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from faker import Faker
import random
from datetime import datetime, timedelta

# Database connection
try:
    user = 'postgres'
    password = '123'
    host = 'localhost'
    port = '5432'
    database = 'Medcine'

    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    connection = engine.connect()
    print("Подключение к базе данных прошло успешно.")
except SQLAlchemyError as e:
    print(f"Ошибка подключения к базе данных: {e}")

# Initialize Faker for generating random names and dates
faker = Faker()

# Parameters
num_records = 1000000  # Number of records to generate
start_date = datetime(2021, 1, 1)
end_date = datetime.now()
date_range = (end_date - start_date).days

# Research data generation
modality_list = ['X-ray', 'MRI', 'CT', 'Ultrasound']
research_type_list = ['Head', 'Chest', 'Abdomen', 'Pelvis', 'Spine']

research_data = []

for _ in range(num_records):
    date = start_date + timedelta(days=random.randint(0, date_range))
    modality = random.choice(modality_list)
    research_type = random.choice(research_type_list)
    count = random.randint(1, 50)
    research_data.append((date, modality, research_type, count))

# Convert to DataFrame
research_df = pd.DataFrame(research_data, columns=['date', 'modality', 'research_type', 'count'])

# Insert data into PostgreSQL
try:
    research_df.to_sql('research_data', engine, if_exists='append', index=False)
    print("Запись данных в таблицу research_data прошла успешно.")
except SQLAlchemyError as e:
    print(f"Ошибка записи данных в таблицу research_data: {e}")

# Doctors data generation
num_doctors = 100
competency_list = [
    {"modality": "X-ray", "research_types": ["Head", "Chest"]},
    {"modality": "MRI", "research_types": ["Abdomen", "Pelvis"]},
    {"modality": "CT", "research_types": ["Spine", "Chest"]},
    {"modality": "Ultrasound", "research_types": ["Abdomen", "Pelvis"]}
]
schedule_list = [
    {"shift": "day", "pattern": "5-2"},
    {"shift": "night", "pattern": "4-3"},
    {"shift": "24h", "pattern": "2-2-3"}
]

doctors_data = []

for _ in range(num_doctors):
    name = faker.name()
    competency = random.choice(competency_list)
    schedule = random.choice(schedule_list)
    doctors_data.append((name, competency, schedule))

# Convert to DataFrame
doctors_df = pd.DataFrame(doctors_data, columns=['name', 'competency', 'schedule'])

# Insert data into PostgreSQL
try:
    doctors_df.to_sql('doctors', engine, if_exists='append', index=False, dtype={'competency': sqlalchemy.types.JSON, 'schedule': sqlalchemy.types.JSON})
    print("Запись данных в таблицу doctors прошла успешно.")
except SQLAlchemyError as e:
    print(f"Ошибка записи данных в таблицу doctors: {e}")

# Close connection
try:
    connection.close()
    print("Соединение с базой данных закрыто.")
except SQLAlchemyError as e:
    print(f"Ошибка при закрытии соединения с базой данных: {e}")
