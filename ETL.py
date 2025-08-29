import pandas as pd
from conection import engine
from sqlalchemy import text

# 1. Leer dataset
df = pd.read_csv("data/candidates.csv", sep=";")

# 2. Transformaciones
df["Application Date"] = pd.to_datetime(df["Application Date"], errors="coerce")
df["hired"] = ((df["Code Challenge Score"] >= 7) & 
               (df["Technical Interview Score"] >= 7)).astype(int)

# 3. Crear tablas dimensionales
dim_technology = pd.DataFrame({"technology": df["Technology"].dropna().unique()})
dim_technology["technology_id"] = dim_technology.index + 1

dim_seniority = pd.DataFrame({"seniority": df["Seniority"].dropna().unique()})
dim_seniority["seniority_id"] = dim_seniority.index + 1

dim_country = pd.DataFrame({"country": df["Country"].dropna().unique()})
dim_country["country_id"] = dim_country.index + 1

dim_date = df[["Application Date"]].dropna().drop_duplicates().rename(columns={"Application Date": "full_date"})
dim_date["year"] = dim_date["full_date"].dt.year
dim_date["month"] = dim_date["full_date"].dt.month
dim_date = dim_date.reset_index(drop=True)
dim_date["date_id"] = dim_date.index + 1

dim_candidate = df[["First Name", "Last Name", "Email", "YOE"]].drop_duplicates().copy()
dim_candidate = dim_candidate.rename(columns={
    "First Name": "first_name",
    "Last Name": "last_name",
    "Email": "email",
    "YOE": "yoe"
})
dim_candidate["candidate_id"] = dim_candidate.index + 1

# 4. Crear tabla de hechos
fact_selection = df.merge(dim_candidate, left_on="Email", right_on="email") \
    .merge(dim_country, left_on="Country", right_on="country") \
    .merge(dim_technology, left_on="Technology", right_on="technology") \
    .merge(dim_seniority, left_on="Seniority", right_on="seniority") \
    .merge(dim_date, left_on="Application Date", right_on="full_date")

fact_selection = fact_selection[[
    "candidate_id",
    "country_id",
    "technology_id",
    "seniority_id",
    "date_id",
    "Code Challenge Score",
    "Technical Interview Score",
    "hired"
]].rename(columns={
    "Code Challenge Score": "code_challenge_score",
    "Technical Interview Score": "interview_score"
})
fact_selection["selection_id"] = fact_selection.index + 1

# 5. Eliminar la tabla de hechos antes de reemplazar dimensiones
with engine.connect() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
    conn.execute(text("DROP TABLE IF EXISTS fact_selection;"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

# 6. Cargar dimensiones (replace)
dim_technology.to_sql('dim_technology', engine, if_exists='replace', index=False)
dim_seniority.to_sql('dim_seniority', engine, if_exists='replace', index=False)
dim_country.to_sql('dim_country', engine, if_exists='replace', index=False)
dim_date.to_sql('dim_date', engine, if_exists='replace', index=False)
dim_candidate.to_sql('dim_candidate', engine, if_exists='replace', index=False)

# 7. Cargar tabla de hechos (replace)
fact_selection.to_sql('fact_selection', engine, if_exists='replace', index=False)

print("¡Carga a MySQL completada!")