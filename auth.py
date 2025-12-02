import streamlit as st
from requests_oauthlib import OAuth2Session
import os

CLIENT_ID = st.secrets["google"]["client_id"]
CLIENT_SECRET = st.secrets["google"]["client_secret"]
REDIRECT_URI = st.secrets["google"]["redirect_uri"]

AUTHORIZATION_BASE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
USER_INFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"


def login():
    oauth = OAuth2Session(
        CLIENT_ID,
        redirect_uri=REDIRECT_URI,
        scope=["openid", "email", "profile"]
    )

    authorization_url, state = oauth.authorization_url(
        AUTHORIZATION_BASE_URL,
        access_type="offline",
        prompt="select_account"
    )

    st.session_state["oauth_state"] = state

    st.write("### Login with Google")
    st.markdown(f"[Click here to Sign In]({authorization_url})")


def callback():
    if "code" not in st.query_params:
        st.stop()

    oauth = OAuth2Session(
        CLIENT_ID,
        state=st.session_state["oauth_state"],
        redirect_uri=REDIRECT_URI
    )

    token = oauth.fetch_token(
        TOKEN_URL,
        client_secret=CLIENT_SECRET,
        code=st.query_params["code"]
    )

    user_info = oauth.get(USER_INFO_URL).json()

    st.session_state["user"] = user_info
