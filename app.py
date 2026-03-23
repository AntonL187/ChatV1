import streamlit as st

st.title("💬 Chat automatique avec fichiers")

uploaded_files = st.file_uploader(
    "📎 Dépose tes fichiers ici",
    accept_multiple_files=True
)

if "messages" not in st.session_state:
    st.session_state.messages = []

user_input = st.chat_input("Tape ton message ici…")

if user_input:
    st.session_state.messages.append(("user", user_input))
    response = f"Tu as dit : {user_input}"
    if uploaded_files:
        response += f"\nFichiers reçus : {[f.name for f in uploaded_files]}"
    st.session_state.messages.append(("bot", response))

for role, msg in st.session_state.messages:
    if role == "user":
        st.chat_message("user").write(msg)
    else:
        st.chat_message("assistant").write(msg)