import gspread
import base64
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from oauth2client.service_account import ServiceAccountCredentials
import pandas_gbq
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pyrebase
import firebase_admin
from firebase_admin import credentials
from firebase_admin import auth
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./spheric-terrain-312804-72bf393d2982.json"

from transformers import pipeline
classifier = pipeline("zero-shot-classification",
                      model="valhalla/distilbart-mnli-12-9")

firebaseConfig = {
    "apiKey": "AIzaSyAzkeqE3Dyc5R3-dBRHawFUmZ6r5zYCnSk",
    "authDomain": "spheric-terrain-312804.firebaseapp.com",
    'databaseURL': "https://spheric-terrain-312804.firebaseio.com",
    "projectId": "spheric-terrain-312804",
    "storageBucket": "spheric-terrain-312804.appspot.com",
    "messagingSenderId": "342212851432",
    "appId": "1:342212851432:web:c1b364fcbeb38d432afb5d"
}

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./spheric-terrain-312804-72bf393d2982.json', scope)
client = gspread.authorize(creds)
sheet = client.open('UNIQLO - Facebook Ad Export (1-30 June 2021)')
sheet_instance = sheet.get_worksheet(0)
records_data = sheet_instance.get_all_records()

df_uniqlo = pd.DataFrame(records_data[1:], columns=records_data[0])
df_uniqlo = df_uniqlo[df_uniqlo['Ad Body'] != '']
df_uniqlo = df_uniqlo.replace('', 0)
df_uniqlo['Cost'] = df_uniqlo['Cost'].astype('float')
df_uniqlo['Impressions'] = df_uniqlo['Impressions'].astype('int')
df_uniqlo['Link Clicks'] = df_uniqlo['Link Clicks'].astype('int')
df_uniqlo['Purchases Conversion Value'] = df_uniqlo['Purchases Conversion Value'].astype('float')

project_id = "spheric-terrain-312804"

sql = """
SELECT *
FROM `fv.ad_copy`
"""
df_gbq = pandas_gbq.read_gbq(sql, project_id=project_id)

def download_link(object_to_download, download_filename, download_link_text):
    if isinstance(object_to_download,pd.DataFrame):
        object_to_download = object_to_download.to_csv(index=False)

    # some strings <-> bytes conversions necessary here
    b64 = base64.b64encode(object_to_download.encode()).decode()

    return f'<a href="data:file/txt;base64,{b64}" download="{download_filename}">{download_link_text}</a>'

def main1():
    roas = df_gbq.groupby('Ad_Copy_Category')[['Purchase_conversion_value', 'Cost']].agg('sum')
    roas['ROAS'] = round(roas['Purchase_conversion_value']/roas['Cost'], 2)
    roas = roas.sort_values('ROAS', ascending=True)
    fig = px.bar(roas[:15], y=roas.index[:15], x='ROAS', text='ROAS', orientation='h', title='Ranking of Ad Copy Category by ROAS')
    st.plotly_chart(fig)

    ad_df = df_gbq.groupby(['Ad_body', 'Ad_Copy_Category'])[['Impressions', 'Link_clicks', 'Purchase_conversion_value', 'Cost']].sum()
    ad_df['CTR'] = ad_df['Link_clicks']/ad_df['Impressions']*100
    ad_df['ROAS'] = ad_df['Purchase_conversion_value']/ad_df['Cost']
    ad_df = ad_df[['CTR', 'ROAS']].reset_index()
    st.subheader('Ad Copy with its Category')
    st.write(ad_df)
    if st.button('Download Ad Copy Category Table'):
        tmp_download_link = download_link(ad_df, 'Ad_label.csv', 'Click here to download your data!')
        st.markdown(tmp_download_link, unsafe_allow_html=True)

    df_gbq['ROAS'] = df_gbq['Purchase_conversion_value']/df_gbq['Cost']
    pivot_df = pd.pivot_table(df_gbq, values=['ROAS'], index=['Ad_set_name', 'Campaign_name'], columns='Ad_Copy_Category', aggfunc=np.sum)
    st.subheader('Best Ad Copy Category with Ad Set and Campaign by ROAS')
    st.write(pivot_df)
    if st.button('Download Best Ad Copy Category by ROAS Table'):
        tmp_download_link = download_link(pivot_df, 'ROAS_ad_label.csv', 'Click here to download your data!')
        st.markdown(tmp_download_link, unsafe_allow_html=True)

def main2(tags, email):
    data = df_uniqlo['Ad Body'].unique()
    result_lst = []
    for text in data:
        try:
            result_lst.append([text, classifier(text, tags)['labels'][0]])
        except:
            result_lst.append([text, 'No Ad Content'])

    result = pd.DataFrame(result_lst, columns=['Ad Body', 'Ad Copy Category'])

    df = pd.merge(df_uniqlo, result, on='Ad Body', how='left')

    df['Cost'] = df['Cost'].astype('float')
    df['Purchases Conversion Value'] = df['Purchases Conversion Value'].replace('', 0)
    df['Purchases Conversion Value'] = df['Purchases Conversion Value'].astype('float')

    df['ROAS'] = round(df['Purchases Conversion Value']/df['Cost'], 2)

    roas = df.groupby('Ad Copy Category')[['Purchases Conversion Value', 'Cost']].agg('sum')
    roas['ROAS'] = roas['Purchases Conversion Value']/roas['Cost']
    roas = roas.sort_values('ROAS', ascending=True)
    fig = px.bar(roas[:15], y=roas.index[:15], x='ROAS', text='ROAS', orientation='h', title='Ranking of Ad Copy Category by ROAS')
    st.plotly_chart(fig)

    ad_df = df.groupby(['Ad Body', 'Ad Copy Category'])[['Impressions', 'Link Clicks', 'Purchases Conversion Value', 'Cost']].sum()
    ad_df['CTR'] = ad_df['Link Clicks']/ad_df['Impressions']*100
    ad_df['ROAS'] = ad_df['Purchases Conversion Value']/ad_df['Cost']
    ad_df = ad_df[['CTR', 'ROAS']].reset_index()
    st.subheader('Ad Copy with its Category')
    st.dataframe(ad_df)
    if st.button('Download Ad Copy Category Table'):
        tmp_download_link = download_link(ad_df, 'Ad_label.csv', 'Click here to download your data!')
        st.markdown(tmp_download_link, unsafe_allow_html=True)

    pivot_df = pd.pivot_table(df, values=['ROAS'], index=['Ad Set Name', 'Campaign Name'], columns='Ad Copy Category', aggfunc=np.sum)
    st.subheader('Best Ad Copy Category with Ad Set and Campaign by ROAS')
    st.dataframe(pivot_df)
    if st.button('Download Best Ad Copy Category by ROAS Table'):
        tmp_download_link = download_link(pivot_df, 'ROAS_ad_label.csv', 'Click here to download your data!')
        st.markdown(tmp_download_link, unsafe_allow_html=True)

    #start email send
    def sendNotif(output,receiver_address,email_subject):
        try:
            mail_content = output
            #The mail addresses and password
            sender_address = 'jiaminglow@impersuasion.com'
            sender_pass = 'iztlgsuqkfpljsfw'
            #Setup the MIME
            message = MIMEMultipart()
            message['From'] = sender_address
            message['To'] = receiver_address
            message['Subject'] = email_subject   #The subject line
            #The body and the attachments for the mail
            message.attach(MIMEText(mail_content, 'plain'))
            #Create SMTP session for sending the mail
            session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
            session.starttls() #enable security
            session.login(sender_address, sender_pass) #login with mail_id and password
            text = message.as_string()
            session.sendmail(sender_address, receiver_address, text)
            session.quit()
        except Exception as E:
            print(str(E))

    email_content = "Please click the following link to get back to dashboard: ##link##"
    sendNotif(email_content, email, "Streamlit Dashboard is ready!")
    #end email send

def login():
    
    menu = ["Login", "SignUp"]
    choice = st.sidebar.selectbox("Menu", menu)
    
    if choice == "Login":
        email = st.sidebar.text_input("Email")
        password = st.sidebar.text_input("Password", type='password')
        
        if st.sidebar.checkbox("Login"):
            firebase = pyrebase.initialize_app(firebaseConfig)
            auth = firebase.auth()
            login = auth.sign_in_with_email_and_password(email, password)
            token = login['idToken']

            if token:
                st.title("UNIQLO's Facebook Ad Analysis")
                st.text('### Instructions ###')
                st.subheader("The chart loaded by default. Enter some text: (Example: Good, Fashion, Sad)")
                label_lst = st.text_input(label='')
                tags = label_lst.replace(" ", "").split(",")
                
                if len(tags) <= 1:
	                main1()
                else:
	                main2(tags, email)
                    
            else:
                st.warning("Incorrect Username/Password")
                
    elif choice == "SignUp":
        st.subheader("Create New Account")
        new_user = st.text_input("Email")
        new_password = st.text_input("Password",type='password')

login()