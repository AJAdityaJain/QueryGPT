import streamlit as st
import ollama
import re
import pandas as pd
from sqlalchemy import create_engine,URL,text

# conn = create_engine(user='admin', password='', database='pizza', host='localhost')
SQLALCHEMY_DATABASE_URL = URL.create(
    "mysql",
    username="admin",
    password='',
    host="localhost",
    # port=5432,
    database="pizza",
)
engine = create_engine("mysql+pymysql://admin:@localhost/pizza")
schema = """

    TABLE pizza_orders (
        id INT PRIMARY KEY AUTO INCREMENT,
        order_id VARCHAR(255),
        pizza_name VARCHAR(255),
        size VARCHAR(10) [S M L XL(extra large) XXL(extra extra large)],
        type VARCHAR(50) [classic veggie chicken supreme],
        price FLOAT,
        order_datetime DATETIME
    )
"""

sys_msg = f"You are a data chatbot. You take in a user query, translate it into MySQL Query. Surround your response with <result></result> tags. Schema: {schema}"

def execsql():
    print('\n',st.session_state.response)
    try:
        st.session_state.data = pd.read_sql(st.session_state.response,engine)
    except:
        print('Error occured')
        with engine.connect() as conn:
            conn.execute(text(st.session_state.response))
            conn.commit()      
        st.session_state.data = pd.DataFrame(data=['Executed Successfully'])  


def respond():
    print('\n###################################\n')
    stream  = ollama.chat(model='phi3', messages=[
    {
        'role': 'system',
        'content': sys_msg,
    },
    {
        'role': 'user',
        'content': st.session_state.user_input,
    },
    ],stream=True)
    
    st.session_state.response = ""
    for chunk in stream:
        st.session_state.response += chunk['message']['content']
        print(chunk['message']['content'], end='', flush=True)

    st.session_state.response = st.session_state.response.split('<result>')[1].split('</result>')[0].replace('%','%%')
    match = re.search(r'(?<=```)(\s|.)*(?=```)',st.session_state.response)
    if match:
        st.session_state.response = match.group()
    st.session_state.response = st.session_state.response.removeprefix('sql')

    if None == re.match(r'\A\s*(delete|drop|truncate)',st.session_state.response,re.IGNORECASE):
        execsql()
    else:
        st.session_state.data = pd.DataFrame(data=['Confirm Operation'])  
        print('DANGER')
        pass

def init_app():
    st.set_page_config(
        page_title="QueryGPT",
        page_icon="📅",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={}
    )
    st.markdown("""
        <style>
                *{
                white-space: pre-wrap;
                }

        .input-container {
            margin-top: 20px;
        }
        .block-container {
            padding-top: 0 !important;
        }
        </style>
        """, unsafe_allow_html=True)
    
    if "display" not in st.session_state:
        st.session_state.display = "Raw"
    if "response" not in st.session_state:
        st.session_state.response = ""
    if "data" not in st.session_state:
        st.session_state.data = None
    st.title("QueryGPT")




    input_container = st.container()
    with input_container:
        st.markdown('<div class="input-container">', unsafe_allow_html=True)
        input, button = st.columns([10,1])
        input.text_input('',placeholder="Type message  :", key='user_input')
        button.button('✅',on_click=respond)
        st.markdown('</div>', unsafe_allow_html=True)

    if st.session_state.display == 'Raw':
        if st.session_state.data is not None:
            arg = st.session_state.data.to_json(orient='records')
        else:
            arg = "{}"
        st.json(arg)
    elif st.session_state.display == 'Table':
        st.dataframe(st.session_state.data)
        
    st.session_state.display = st.sidebar.selectbox(
    "Display it as?",
    ("Raw", "Table"))
    st.sidebar.code(st.session_state.response,language='sql')
    st.sidebar.button('▶️',on_click=execsql)





if __name__ == '__main__':
    init_app()
    