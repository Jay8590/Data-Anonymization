# Data Anonymization using Streamlit

![Screenshot 2023-11-04 at 21-47-13 main · Streamlit](https://github.com/Jay8590/Data-Anonymization/assets/53402742/1c997826-e22f-43a8-bba8-0a30bcbf5891)

## Description

This application is built using Streamlit and allows for data anonymization. It provides different functionalities for administrators and regular users.

- ***Administrators*** can:
  - Mask Aadhar Card and PAN Card details of selected users.
  - Save the masked data in the database.

- Regular users can:
  - View data from the database.

## Installation

1. Clone the repository:

   ```
   git clone https://github.com/Jay8590/Data-Anonymization
    ```
   
2. Install the required Python packages:
   ```
    pip install -r requirements.txt
   ```
   
3. Configure your ***config.yaml*** file with the necessary credentials and database information.

## Usage
    

1. Run the Streamlit app:

    ```  
    streamlit run app.py
    ```
    
2. Log in using your ***username*** and ***password***.

3. Based on your role (admin or regular user), you can perform different actions.

4. Follow the on-screen instructions to mask data, view data, or perform other tasks.

## Configuration

  Modify the ***config.yaml*** file to configure the application with the necessary credentials and settings.
