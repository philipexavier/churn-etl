import pandas as pd
import os
from sqlalchemy import create_engine

# Supabase conn (substitua YOUR_SUPABASE_URL/KEY)
DB_URL = "postgresql://postgres:[SENHA]@db.[PROJECT].supabase.co:5432/postgres"
engine = create_engine(DB_URL)

# Netflix ETL
df_net = pd.read_csv("netflix_customer_churn.csv")
df_net["activity_score"] = df_net["watchhours"] / (df_net["lastlogindays"] + 1)
df_net["churn"] = df_net["churned"].astype(int)
df_net.to_sql("netflix_churn", engine, if_exists="replace", index=False)

# Banco ETL
df_bank = pd.read_csv("Churn_Modelling.csv")
df_bank["activity_score"] = df_bank["Tenure"] / (df_bank["Age"] + 1)  # Exemplo proxy
df_bank.to_sql("bank_churn", engine, if_exists="replace", index=False)

print("ETL concluído!")
