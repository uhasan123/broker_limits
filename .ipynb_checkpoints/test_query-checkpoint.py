import os
import pandas as pd
import streamlit as st

from broker_report import broker_report

obj=broker_report()
st.write(obj.invoice_table())