import pandas as pd
import os
from sqlalchemy import create_engine

DB_URL = os.getenv("SUPABASE_DB_URL")  # Use env var
engine = create_engine(DB_URL)

# Netflix ETL
df_net = pd.read_csv("/app/netflix_customer_churn.csv")  # Caminho absoluto
print(df_net.columns.tolist())  # Debug cols
df_net["activity_score"] = df_net["watch_hours"] / (df_net["last_login_days"] + 1)
df_net["churn"] = df_net["churned"].astype(int)
df_net.to_sql("netflix_churn", engine, if_exists="replace", index=False)

# Banco ETL (Exited existe)
df_bank = pd.read_csv("/app/Churn_Modelling.csv")
df_bank["activity_score"] = (
    df_bank["Tenure"] * 30 / (df_bank["Age"] + 1)
)  # Proxy mensal
df_bank.to_sql("bank_churn", engine, if_exists="replace", index=False)

print("ETL concluído! Netflix rows:", len(df_net), "Bank rows:", len(df_bank))
