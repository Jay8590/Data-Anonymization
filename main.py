import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import mysql.connector
import logging
import pandas as pd
import random

# Set up logging for the application
logging.basicConfig(level=logging.INFO)

# Configuration file path
CONFIG_FILE = 'config.yaml'
# Administrator's name
ADMIN_NAME = 'Jay K'

# Load configuration from 'config.yaml'
def load_config(file):
    with open(file) as f:
        return yaml.load(f, Loader=SafeLoader)

# Create a MySQL connection
def create_mysql_connection(host, username, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
        )

        if connection.is_connected():
            print("Connected to MySQL server")
            
            # Create the database if it doesn't exist
            cursor = connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cursor.close()
            
            connection.database = database
            
            return connection
    except mysql.connector.Error as error:
        print(f"Error: {error}")
        return None
    return None

# Insert users from the configuration file into the database
def insert_users_from_config(connection, config):
    try:
        cursor = connection.cursor()
        users = config['credentials']['usernames']
        
        # Check if the 'Users' table exists
        cursor.execute("SHOW TABLES LIKE 'Users'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            create_table_query = """
            CREATE TABLE Users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                aadhar_card_number VARCHAR(255),
                pan_card_number VARCHAR(255)
            )
            """
            cursor.execute(create_table_query)
            
            for username, user_data in users.items():
                # Insert user data into the 'Users' table
                insert_query = "INSERT INTO Users (name, email, aadhar_card_number, pan_card_number) VALUES (%s, %s, %s, %s)"
                user_info = (user_data['name'], user_data['email'], user_data.get('aadhar_card_number', None), user_data.get('pan_card_number', None))
                cursor.execute(insert_query, user_info)

            connection.commit()
            logging.info("Users inserted successfully (including additional fields).")
        else:
            logging.info("Users table already exists; skipping insertion.")

        cursor.close()
    except mysql.connector.Error as error:
        logging.error(f"Error inserting users: {error}")
        connection.rollback()

# Mask data based on the column type
def mask_data(data, column_type):
    if column_type == 'aadhar':
        # Implement masking logic for Aadhar Card
        if data is not None:
            return 'XXXX-XXXX-' + data[-4:]  # Mask the last 4 digits
        else:
            return None
    elif column_type == 'pan':
        # Implement masking logic for PAN Card
        if data is not None:
            return 'XXXXXX' + data[-4:]  # Mask all characters except the last 4
        else:
            return None
    else:
        return data

# Authenticate the user
def authenticate_user(authenticator, app_name):
    name, authentication_status, username = authenticator.login('Login', app_name)
    return name, authentication_status, username

# Display user details in a DataFrame
def display_user_details(connection, mask_aadhar, mask_pan, selected_users, admin):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM Users")
    users = cursor.fetchall()
    cursor.close()

    if users:
        st.header('User Details')
        
        user_details = []
        
        for user in users:
            if user[1] in selected_users:
                aadhar = mask_data(user[3], 'aadhar') if mask_aadhar else user[3]
                pan = mask_data(user[4], 'pan') if mask_pan else user[4]
            else:
                aadhar = user[3]
                pan = user[4]
            
            user_details.append([user[1], user[2], aadhar, pan])
        
        # Display the user details in a DataFrame
        user_df = pd.DataFrame(user_details, columns=['Name', 'Email', 'Aadhar Card', 'PAN Card'])
        st.dataframe(user_df)
    else:
        st.warning('No users found in the database.')

# Admin section for masking data
def admin_section(connection):
    st.subheader('Admin Section')
    mask_aadhar = st.checkbox('Mask Aadhar Card')
    mask_pan = st.checkbox('Mask PAN Card')

    cursor = connection.cursor()
    cursor.execute("SELECT name FROM Users")
    user_names = [user[0] for user in cursor.fetchall()]
    cursor.close()

    st.subheader('Select Users to Mask:')
    selected_users = st.multiselect('Select users to mask:', user_names)

    return mask_aadhar, mask_pan, selected_users

# Save masked data in the database
def save_masked_data(connection, selected_users, mask_aadhar, mask_pan):
    try:
        cursor = connection.cursor()
        for username in selected_users:
            cursor.execute("SELECT * FROM Users WHERE name = %s", (username,))
            user = cursor.fetchone()
            if user:
                aadhar_masked = mask_data(user[3], 'aadhar') if mask_aadhar else user[3]
                pan_masked = mask_data(user[4], 'pan') if mask_pan else user[4]

                cursor.execute("UPDATE Users SET aadhar_card_number = %s, pan_card_number = %s WHERE name = %s",
               (aadhar_masked, pan_masked, username))

        connection.commit()
        logging.info("Masked data saved successfully.")
    except mysql.connector.Error as error:
        logging.error(f"Error saving masked data: {error}")
        connection.rollback()

# Insert user details into the database
def insert_user_details(connection, name, email, aadhar_card_number, pan_card_number):
    try:
        cursor = connection.cursor()
        
        insert_query = "INSERT INTO Users (name, email, aadhar_card_number, pan_card_number) VALUES (%s, %s, %s, %s)"
                
        user_info = (name, email, aadhar_card_number, pan_card_number)
        cursor.execute(insert_query, user_info)

        connection.commit()
        logging.info(f"User '{name}' added successfully.")
    except mysql.connector.Error as error:
        logging.error(f"Error inserting user details: {error}")
        connection.rollback()

# Define your Streamlit app
def main():
    st.title('Data Anonymization')

    # Load configuration from the 'config.yaml' file
    config = load_config(CONFIG_FILE)
    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )

    # Authenticate the user
    name, authentication_status, username = authenticate_user(authenticator, 'main')

    if authentication_status:
        authenticator.logout('Logout', 'main')
        st.write(f'Welcome *{name}*')
        if name == ADMIN_NAME:
            st.title('ADMINISTRATOR')
            host = config['database']['host']
            username = config['database']['username']
            password = config['database']['password']
            database = config['database']['name']

            connection = create_mysql_connection(host, username, password, database)
            if connection is not None:
                insert_users_from_config(connection, config)

                mask_aadhar, mask_pan, selected_users = admin_section(connection)

                if selected_users:
                    display_user_details(connection, mask_aadhar, mask_pan, selected_users, True)
                    
                    if st.button('Save Masked Data'):
                        save_masked_data(connection, selected_users, mask_aadhar, mask_pan)
                    
        else:
            host = config['database']['host']
            username = config['database']['username']
            password = config['database']['password']
            database = config['database']['name']
            connection = create_mysql_connection(host, username, password, database)
            
            if connection is not None:
                # Fetch all data from the table
                cursor = connection.cursor()
                cursor.execute("SELECT * FROM Users")
                data = cursor.fetchall()

            if data:
                # Create a Pandas DataFrame
                df = pd.DataFrame(data, columns=[i[0] for i in cursor.description])

                # Display the DataFrame in Streamlit
                st.dataframe(df)
            else:
                st.warning('No data found in the database.')

            cursor.close()

    elif authentication_status == False:
        st.error('Username/password is incorrect')
    elif authentication_status is None:
        st.warning('Please enter your username and password')

if __name__ == '__main__':
    main()
