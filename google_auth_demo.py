import streamlit as st
from google_auth_oauthlib.flow import Flow
from google.oauth2 import id_token
from google.auth.transport import requests
import json
import os

st.set_page_config(page_title="Google Login Demo")

# -------------------------#
#  LOAD CREDENTIALS
# -------------------------#
CLIENT_ID = st.secrets["google_oauth"]["client_id"]
CLIENT_SECRET = st.secrets["google_oauth"]["client_secret"]
REDIRECT_URI = st.secrets["google_oauth"]["redirect_uri"]

# Use OAuth 2.0 "Flow"
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


# -------------------------#
#  MAIN LOGIC
# -------------------------#
def google_login():
    auth_url, _ = flow.authorization_url(prompt="consent")
    st.markdown(f"[🔐 Login with Google]({auth_url})")


def fetch_user_info(code):
    flow.fetch_token(code=code)
    creds = flow.credentials
    request = requests.Request()

    id_info = id_token.verify_oauth2_token(
        creds._id_token, request, CLIENT_ID
    )

    return {
        "email": id_info["email"],
        "name": id_info.get("name"),
        "picture": id_info.get("picture"),
    }


st.title("🔐 Google Auth Prototype")

# Get OAuth "code" from URL
query_params = st.query_params
code = query_params.get("code")

if code:
    user = fetch_user_info(code)
    st.query_params.clear()
    if "bobtail.com" not in user['email']:
        st.error("🚫 Access denied. Your email is not authorized.")
    else:
        st.success("Login successful!")
        st.write("Welcome:", user["name"])
        st.write("Email:", user["email"])
        st.image(user["picture"], width=80)
    
        # Store session
        st.session_state["user"] = user

else:
    if "user" in st.session_state:
        st.success(f"Welcome back, {st.session_state['user']['name']}!")
    else:
        google_login()
