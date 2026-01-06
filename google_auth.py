import streamlit as st
from google_auth_oauthlib.flow import Flow
from google.oauth2 import id_token
from google.auth.transport import requests
import json
import os

# st.set_page_config(page_title="Google Login Demo")

# Use OAuth 2.0 "Flow"
def google_flow(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI):
  flow = Flow.from_client_config(
      {
          "web": {
              "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET,
              "auth_uri": "https://accounts.google.com/o/oauth2/auth",
              "token_uri": "https://oauth2.googleapis.com/token",
          }
      },
      scopes=["openid", "https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile"],
      redirect_uri=REDIRECT_URI,
  )
  return flow


# -------------------------#
#  MAIN LOGIC
# -------------------------#
def google_login(flow):
    auth_url, _ = flow.authorization_url(prompt="consent")
    st.markdown(f"[🔐 Login with Google]({auth_url})")


def fetch_user_info(code, flow):
    flow.fetch_token(code=code)
    creds = flow.credentials
    request = requests.Request()

    id_info = id_token.verify_oauth2_token(
        creds._id_token, request, audience=flow.client_config["client_id"]
    )

    return {
        "email": id_info["email"],
        "name": id_info.get("name"),
        "picture": id_info.get("picture"),
    }


def login():
  if "authenticated" not in st.session_state:
      st.session_state.authenticated = False
  if st.session_state.authenticated:
      return True
  # st.title("🔐 Exhaustion Monitoring Dashboard")
  
  CLIENT_ID = st.secrets["google_oauth"]["client_id"]
  CLIENT_SECRET = st.secrets["google_oauth"]["client_secret"]
  REDIRECT_URI = st.secrets["google_oauth"]["redirect_uri"]
  flow=google_flow(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)

  auth_url, _ = flow.authorization_url(prompt="consent")
  st.markdown(f"[🔐 Login with Google]({auth_url})")

  query_params = st.experimental_get_query_params()
  # query_params = st.query_params
  # code = query_params.get("code")
  
  if "code" in query_params:
    code = query_params.get("code")
    user = fetch_user_info(code, flow)
    st.query_params.clear()
    st.session_state.authenticated = True
    st.session_state["user"] = user
    st.experimental_set_query_params()
    st.experimental_rerun()
  return False
  
  # else:
  #     if "user" in st.session_state:
  #       st.success(f"Welcome back, {st.session_state['user']['name']}!")
  #     else:
  #       google_login(flow)
