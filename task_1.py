import os
from datetime import datetime
import requests
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import time
import concurrent.futures

upoloaded_file=st.file_uploader("Загрузите файл с историческими данныи (CSV)", type="csv")
if upoloaded_file is not None:
    data=pd.read_csv(upoloaded_file, parse_dates=["timestamp"])
else:
    st.info("Загрузите файл temperature_data.cvs")
    st.stop()

if st.button("Сравнить скорость асинх и не асинх"):
    cc=data["city"].unique()
    window=30
    start=time.time()
    for c in cc:
        df=data[data["city"]==c]
        df["temperature"].rolling(window).mean()
    t1=time.time()-start

    start=time.time()
    def calc(c):
        df=data[data["city"]==c]
        return df["temperature"].rolling(window).mean()
    with concurrent.futures.ThreadPoolExecutor() as ex:
        list(ex.map(calc, cc))
    t2=time.time()-start
    col1, ccol2=st.columns(2)
    col1.metric("Без паралелей", f"{t1:.3f} сек")
    ccol2.metric("С Параллелями", f"{t2:.3f} сек")
    st.success(f"Ускорение: {t1/t2:.2f}x")
st.divider()

slovarik={12: "winter",11: "autumn",10: "autumn",9: "autumn",8: "summer",7: "summer",6: "summer",5: "spring",4: "spring",3: "spring",2: "winter",1: "winter" }



data["season"]=data["timestamp"].dt.month.map(slovarik)
c=st.selectbox("Город", data["city"].unique())
c_data=data[data["city"]==c].sort_values("timestamp")

window =30
c_data["ma"]=c_data["temperature"].rolling(window, min_periods=1).mean()
c_data["ms"]=c_data["temperature"].rolling(window, min_periods=1).std()
c_data["anomaly"]=np.abs(c_data["temperature"]- c_data['ma'])>2*c_data["ms"]

if st.button("Описательная статистика"):
    st.dataframe(c_data["temperature"].describe().round(1).to_frame().T, use_container_width=True)

if st.button("Временной ряд с анамалиями"):
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=c_data["timestamp"], y=c_data["temperature"], mode="lines", name="Темпеоатура", line=dict(color="gray")))
    fig.add_trace(go.Scatter(x=c_data['timestamp'], y=c_data["ma"], mode='lines', name="MA(30)", line=dict(color="red")))
    anomally=c_data[c_data["anomaly"]]
    fig.add_trace(go.Scatter(x=anomally["timestamp"], y=anomally["temperature"], mode="markers",name="Аномалии", marker=dict(color="red")))
    st.plotly_chart(fig, use_container_width=True)

if st.button("Сезонный профиль"):
    sessional=c_data.groupby("season")["temperature"].mean().reindex(["winter", "spring", "summer", "autumn"])
    st.plotly_chart(px.bar(sessional, title="Сезонный профиль"), use_container_width=True)

if st.button("Отобразить тренды"):
    yearly=c_data.groupby(c_data["timestamp"].dt.year)["temperature"].mean().reset_index()
    yearly.columns=["year", "temp"]
    if len(yearly)>1:
        s, i = np.polyfit(yearly["year"], yearly["temp"], 1)
        corr=np.corrcoef(yearly["year"], yearly["temp"])[0,1]
        figg=go.Figure()
        figg.add_trace(go.Scatter(x=yearly["year"], y=yearly["temp"], mode="lines+markers", name="Среднегодавая"))
        figg.add_trace(go.Scatter(x=yearly["year"], y=s*yearly["year"]+i, mode="lines", name=f"Тренд {s*10:.1f} за 10 лет"))
        st.plotly_chart(figg, use_container_width=True)


api_key=st.text_input("Введите API ключ OpenWeatherMap", type="password")
if st.button("Получить актуальную температуру и сравнить с историческим диапазоном"):
    if not api_key:
        st.error("Не подходит")
    else:
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={c}&appid={api_key}&units=metric"
            q=requests.get(url, timeout=10)
            if q.status_code==401:
                st.error("Неверный Api")
            elif q.status_code==200:
                seichas=q.json()["main"]["temp"]
                sezon=slovarik[datetime.now().month]
                data_vsm_da=data[(data["city"]==c)&(data["season"]==sezon)]
                mean=data_vsm_da["temperature"].mean()
                std=data_vsm_da["temperature"].std()
                l=mean-2*std
                u=mean+2*std
                st.metric("Текущая", f"{seichas}")
                st.metric("Норма", f"{mean:.1f}")
                if seichas<l:
                    st.error(f"Холоднее нормы на {l-seichas:.1f}")
                elif seichas>u:
                    st.error(f"Горячее нормы на {seichas - u:.1f}")
                else:
                    st.write("Температура в норме")
        except Exception as e:
            st.error(f"Ошибка подключения: {e}")
