import pandas as pd
import plotly.express as px
import requests
import streamlit as st

st.title("Dashboard de Funcionários - Cassandra")

API_URL = "http://localhost:3000"

st.header("Buscar funcionários por gerente")
manager = st.text_input("Nome do gerente")
if st.button("Buscar por gerente"):
    url = f"{API_URL}/employee_by_manager?manager_name={manager.strip()}"
    resp = requests.get(url)
    if resp.ok:
        df = pd.DataFrame(resp.json())
        st.dataframe(df)
        # Gráfico de distribuição de gênero
        if "gender" in df.columns and not df.empty:
            gender_counts = df["gender"].value_counts().reset_index()
            gender_counts.columns = ["gender", "count"]
            fig_gender = px.bar(gender_counts, x="gender", y="count", title="Distribuição de Gênero (M/F)")
            st.plotly_chart(fig_gender)
    else:
        st.error("Erro ao buscar dados do gerente")

st.header("Buscar funcionários por departamento e data")
department = st.text_input("Nome do departamento")
date = st.text_input("Data (YYYY-MM-DD)")
if st.button("Buscar por departamento e data"):
    url = f"{API_URL}/employee_by_dept?department={department.strip()}&date={date.strip()}"
    resp = requests.get(url)
    if resp.ok:
        df = pd.DataFrame(resp.json())
        st.dataframe(df)
        # Gráfico de distribuição de gênero
        if "gender" in df.columns and not df.empty:
            gender_counts = df["gender"].value_counts().reset_index()
            gender_counts.columns = ["gender", "count"]
            fig_gender = px.bar(gender_counts, x="gender", y="count", title="Distribuição de Gênero (M/F)")
            st.plotly_chart(fig_gender)
    else:
        st.error("Erro ao buscar dados do departamento")

st.header("Média Salarial por Departamento")
if st.button("Mostrar média salarial por departamento"):
    url = f"{API_URL}/media_salarial_por_dept"
    resp = requests.get(url)
    if resp.ok:
        df = pd.DataFrame(resp.json())
        st.dataframe(df)
        if not df.empty:
            # Gráfico de média salarial por departamento
            fig_salario = px.bar(df, x="dept_name", y="media_salario", title="Média Salarial por Departamento")
            st.plotly_chart(fig_salario)
            # Gráfico de total de funcionários por departamento
            fig_total = px.bar(df, x="dept_name", y="total_employees", title="Total de Funcionários por Departamento")
            st.plotly_chart(fig_total)
    else:
        st.error("Erro ao buscar dados de média salarial")
