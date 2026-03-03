import os
import pandas as pd
from sqlalchemy import create_engine

# =========================
# Config DB
# =========================

# Para rodar no Easypanel, usando o container do Supabase:
# coloque essa env no serviço ETL:
# SUPABASE_DB_URL=postgresql://postgres:bfY6NDAVaPRnchAL@big_data_ai_supabase-db-1:5432/postgres
DB_URL = os.getenv(
    "SUPABASE_DB_URL",
    "postgresql://postgres:bfY6NDAVaPRnchAL@big_data_ai_supabase-db-1:5432/postgres",
)

print("DB_URL usada:", DB_URL.replace("bfY6NDAVaPRnchAL", "***"))

engine = create_engine(DB_URL)

# =========================
# Netflix ETL
# =========================

df_net = pd.read_csv("netflix_customer_churn.csv")
print("Netflix cols:", df_net.columns.tolist())

# Garantir nomes em lower case (caso o CSV venha diferente)
df_net.columns = [c.lower() for c in df_net.columns]

# activity_score = watch_hours / (last_login_days + 1)
df_net["last_login_days"] = (
    pd.to_numeric(df_net["last_login_days"], errors="coerce").fillna(0).astype(int)
)
df_net["watch_hours"] = (
    pd.to_numeric(df_net["watch_hours"], errors="coerce").fillna(0).astype(float)
)
df_net["activity_score"] = df_net["watch_hours"] / (df_net["last_login_days"] + 1)

# churn binário
df_net["churn"] = (
    pd.to_numeric(df_net["churned"], errors="coerce").fillna(0).astype(int)
)

df_net.to_sql("netflix_churn", engine, if_exists="replace", index=False)
print(f"Netflix: {len(df_net)} rows, churn rate: {df_net['churn'].mean():.2%}")

# =========================
# Banco ETL
# =========================

df_bank = pd.read_csv("Churn_Modelling.csv")
print("Bank cols:", df_bank.columns.tolist())

# activity_score simples: Tenure * 30 / (Age + 1)
df_bank["Tenure"] = (
    pd.to_numeric(df_bank["Tenure"], errors="coerce").fillna(0).astype(int)
)
df_bank["Age"] = pd.to_numeric(df_bank["Age"], errors="coerce").fillna(0).astype(int)
df_bank["activity_score"] = df_bank["Tenure"] * 30 / (df_bank["Age"] + 1)

# garantir Exited numérico
df_bank["Exited"] = (
    pd.to_numeric(df_bank["Exited"], errors="coerce").fillna(0).astype(int)
)

df_bank.to_sql("bank_churn", engine, if_exists="replace", index=False)
print(f"Bank: {len(df_bank)} rows, churn rate: {df_bank['Exited'].mean():.2%}")

print("ETL concluído! Dados no Supabase.")
