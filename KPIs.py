import pandas as pd
import matplotlib.pyplot as plt
from conection import engine

# 1. Contrataciones por tecnología
query_tech = """
SELECT dt.technology, COUNT(fs.selection_id) AS hires
FROM fact_selection fs
JOIN dim_technology dt ON fs.technology_id = dt.technology_id
WHERE fs.hired = 1
GROUP BY dt.technology
ORDER BY hires DESC;
"""
df_tech = pd.read_sql(query_tech, engine)
print("\n>>> Contrataciones por tecnología")
print(df_tech)
plt.figure(figsize=(10,6))
plt.barh(df_tech["technology"], df_tech["hires"], color='royalblue')
plt.xlabel("Contrataciones")
plt.title("Contrataciones por Tecnología")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# 2. Contrataciones por año
query_year = """
SELECT dd.year, COUNT(fs.selection_id) AS hires
FROM fact_selection fs
JOIN dim_date dd ON fs.date_id = dd.date_id
WHERE fs.hired = 1
GROUP BY dd.year
ORDER BY dd.year;
"""
df_year = pd.read_sql(query_year, engine)
print("\n>>> Contrataciones por año")
print(df_year)
plt.figure(figsize=(7,4))
plt.bar(df_year['year'], df_year['hires'], color='orange')
plt.xlabel('Año')
plt.ylabel('Contrataciones')
plt.title('Contrataciones por Año')
plt.tight_layout()
plt.show()

# 3. Contrataciones por seniority
query_seniority = """
SELECT ds.seniority, COUNT(fs.selection_id) AS hires
FROM fact_selection fs
JOIN dim_seniority ds ON fs.seniority_id = ds.seniority_id
WHERE fs.hired = 1
GROUP BY ds.seniority
ORDER BY hires DESC;
"""
df_seniority = pd.read_sql(query_seniority, engine)
print("\n>>> Contrataciones por seniority")
print(df_seniority)
plt.figure(figsize=(8,5))
plt.barh(df_seniority["seniority"], df_seniority["hires"], color='teal')
plt.xlabel("Contrataciones")
plt.title("Contrataciones por Seniority")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# 4. Contrataciones por país (USA, Brazil, Colombia, Ecuador) por año
query_country = """
SELECT dc.country, dd.year, COUNT(fs.selection_id) AS hires
FROM fact_selection fs
JOIN dim_country dc ON fs.country_id = dc.country_id
JOIN dim_date dd ON fs.date_id = dd.date_id
WHERE fs.hired = 1
  AND dc.country IN ('United States of America', 'Brazil', 'Colombia', 'Ecuador')
GROUP BY dc.country, dd.year
ORDER BY dc.country, dd.year;
"""
df_country = pd.read_sql(query_country, engine)
print("\n>>> Contrataciones por país (United States of America, Brazil, Colombia, Ecuador) por año")
print(df_country)
plt.figure(figsize=(10,6))
for country in df_country['country'].unique():
    data = df_country[df_country['country'] == country]
    plt.plot(data['year'], data['hires'], marker='o', label=country)
plt.xlabel('Año')
plt.ylabel('Contrataciones')
plt.title('Contrataciones por País y Año')
plt.legend()
plt.tight_layout()
plt.show()

# 5. Tasa de contratación por tecnología
query_hire_rate = """
SELECT dt.technology,
       SUM(fs.hired) AS hires,
       COUNT(fs.selection_id) AS total_candidates,
       ROUND(SUM(fs.hired) * 100.0 / COUNT(fs.selection_id), 2) AS hire_rate
FROM fact_selection fs
JOIN dim_technology dt ON fs.technology_id = dt.technology_id
GROUP BY dt.technology
ORDER BY hire_rate DESC;
"""
df_hire_rate = pd.read_sql(query_hire_rate, engine)
print("\n>>> Tasa de contratación por tecnología")
print(df_hire_rate)
plt.figure(figsize=(10,6))
plt.barh(df_hire_rate["technology"], df_hire_rate["hire_rate"], color='purple')
plt.xlabel("Tasa de contratación (%)")
plt.title("Tasa de contratación por Tecnología")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# 6. Puntajes promedio por seniority
query_scores = """
SELECT ds.seniority,
       ROUND(AVG(fs.code_challenge_score), 2) AS avg_code_score,
       ROUND(AVG(fs.interview_score), 2) AS avg_interview_score
FROM fact_selection fs
JOIN dim_seniority ds ON fs.seniority_id = ds.seniority_id
GROUP BY ds.seniority
ORDER BY avg_code_score DESC;
"""
df_scores = pd.read_sql(query_scores, engine)
print("\n>>> Puntajes promedio por seniority")
print(df_scores)
df_scores_melted = df_scores.melt(id_vars='seniority', value_vars=['avg_code_score', 'avg_interview_score'],
                                  var_name='Tipo', value_name='Puntaje')
plt.figure(figsize=(10,6))
import seaborn as sns
sns.barplot(data=df_scores_melted, x='Puntaje', y='seniority', hue='Tipo')
plt.title('Puntajes Promedio por Seniority')
plt.xlabel('Puntaje Promedio')
plt.ylabel('Seniority')
plt.legend(title='Tipo de Puntaje')
plt.tight_layout()
plt.show()