import pandas as pd
import os
from sqlalchemy import create_engine

# Self-hosted Supabase
DB_URL = os.getenv(
    "SUPABASE_DB_URL",
    "postgresql://postgres:eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE@db:5432/postgres",
)
print(
    "DB_URL usada:",
    DB_URL.replace("your-super-secret-and-long-postgres-password", "***"),
)

engine = create_engine(DB_URL)

# Netflix ETL
df_net = pd.read_csv("netflix_customer_churn.csv")
print("Netflix cols:", df_net.columns.tolist())
df_net["activity_score"] = df_net["watch_hours"] / (df_net["last_login_days"] + 1)
df_net["churn"] = df_net["churned"].astype(int)
df_net.to_sql("netflix_churn", engine, if_exists="replace", index=False)
print(f"Netflix: {len(df_net)} rows, churn rate: {df_net['churn'].mean():.2%}")

# Banco ETL
df_bank = pd.read_csv("Churn_Modelling.csv")
df_bank["activity_score"] = df_bank["Tenure"] * 30 / (df_bank["Age"] + 1)
df_bank.to_sql("bank_churn", engine, if_exists="replace", index=False)
print(f"Bank: {len(df_bank)} rows, churn rate: {df_bank['Exited'].mean():.2%}")

print("ETL concluído! Dados no Supabase.")
