import streamlit as st
import ollama
import re
import pandas as pd
import sqlalchemy as sql

#Change accordingly
db = 'world_x'
enum_threshhold = 8

def exec_sql():
    print('\n',st.session_state.response)
    try:
        st.session_state.data = pd.read_sql(st.session_state.response,st.session_state.engine)
    except:
        try:
            print('Error occured')
            with st.session_state.engine.connect() as conn:
                conn.execute(sql.text(st.session_state.response))
                conn.commit()      
            st.session_state.data = pd.DataFrame(data=['Executed Successfully'])  
        except:
            st.session_state.data = pd.DataFrame(data=['Query Failed'])  


def respond():
    print('\n###################################\n')
    stream  = ollama.chat(model='phi3', messages=[
    {
        'role': 'system',
        'content': st.session_state.schema_msg,
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
        exec_sql()
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
    if "engine" not in  st.session_state:
        st.session_state.engine = sql.create_engine("mysql+pymysql://admin:@localhost/"+db)
    if "schema_msg" not in st.session_state:
        st.session_state.schema_msg = get_schema_msg()

    st.title("QueryGPT")




    input_container = st.container()
    with input_container:
        st.markdown('<div class="input-container">', unsafe_allow_html=True)
        input, button = st.columns([10,1])
        input.text_input('',placeholder="Type message  :", key='user_input')
        button.button('✅',on_click=respond)
        st.markdown('</div>', unsafe_allow_html=True)

    st.session_state.display = st.sidebar.selectbox(
    "Display it as?",
    ("Raw", "Table"))

    if st.session_state.data is not None:
        cols = pd.Series(st.session_state.data.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        st.session_state.data.columns = cols

    if st.session_state.display == 'Raw':
        if st.session_state.data is not None:
            arg = st.session_state.data.to_json(orient='records')
        else:
            arg = "{}"
        st.json(arg)
    elif st.session_state.display == 'Table':
        st.dataframe(st.session_state.data)
    st.sidebar.code(st.session_state.response,language='sql')
    st.sidebar.button('▶️',on_click=exec_sql)

def get_schema_msg():
    schema = ""
    inspector = sql.inspect(st.session_state.engine)

    for table_name in inspector.get_table_names(db):
        schema += f'TABLE {table_name} (\n'
        sqltable = sql.Table(table_name,sql.MetaData(),autoload_with=st.session_state.engine)
        for column in sqltable.columns:
            schema += f'    {column.name} {column.type}'
            if len(column.foreign_keys) > 0:
                schema += ' FOREIGN-REFERENCE('
                for fkey in column.foreign_keys:
                    schema += ' ' + fkey._get_colspec()
                schema += ')'
            if column.primary_key == True:
                schema += ' PRIMARY-KEY'
            if column.autoincrement == True:
                schema += ' AUTO-INCREMENT'
            enumval = pd.read_sql(f'SELECT DISTINCT {column.name} FROM {table_name}\nWHERE (SELECT COUNT(DISTINCT {column.name}) FROM {table_name}) < {enum_threshhold};',st.session_state.engine)
            if len(enumval) != 0:
                schema += ' ['
                for val in enumval.values:
                    if val[0] is not None:
                        schema += str(val[0])
                        schema += ' '
                schema += ']'

            schema += ',\n'
        schema += ')\n\n'
    print(schema)
    return f"You take in a user query, translate it into MySQL with respect to Schema. Handle relational queries. Surround your response with <result></result> tag and don't explain. Schema: "+ schema



if __name__ == '__main__':
    init_app()
