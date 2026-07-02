# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dbx_spotify_cat.gold.dimuser
# MAGIC WHERE "__END_AT" IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     user_id,
# MAGIC     user_name,
# MAGIC     subscription_type,
# MAGIC     updated_at,
# MAGIC     __START_AT,
# MAGIC     __END_AT
# MAGIC FROM dbx_spotify_cat.gold.dimuser
# MAGIC WHERE __END_AT IS NOT NULL;

# COMMAND ----------

