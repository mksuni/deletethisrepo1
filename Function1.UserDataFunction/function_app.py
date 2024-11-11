import datetime
import fabric.functions as udf
import logging
import json 

app = udf.FabricApp()

@app.function("hello_fabric")
def hello_fabric(name: str) -> str:
    logging.info('Python UDF trigger function processed a request.')

    return f"Welcome to Fabric Functions, {name}, at {datetime.datetime.now()}!"


# This sample allows you to read data from Azure SQL database 
# Complete these steps before testing this funtion 
#   1. Select Manage connections to connect to Azure SQL database 


@app.fabric_item_input(argName="sqlDB",alias="sqlDB")
@app.function("read_from_azure_sql_db")
def read_from_azure_sql_db(sqlDB: udf.FabricSqlConnection)->str:
    # Replace with the query you want to run
      query = "SELECT * FROM (VALUES ('John Smith', 31), ('Kayla Jones', 33)) AS Employee(EmpName, DepID);"
  
      # Establish a connection to the SQL database
      connection = sqlDB.connect()
      cursor = connection.cursor()
  
      # Execute the query
      cursor.execute(query)
  
      # Fetch all results
      results = []
      for row in cursor.fetchall():
          results.append(row)
  
      # Close the connection
      cursor.close()
      connection.close()  
      resultsJSON = json.dumps({"values": results})          
      return results

import pandas as pd
@app.function("manipulate_data")
def manipulate_data(data: dict)-> str:
    items= data["data"]
    # Convert the data dictionary to a DataFrame
    df = pd.DataFrame(items)
        # Perform basic data manipulation
    # Example: Add a new column 'AgeGroup' based on the 'Age' column    
    df['AgeGroup'] = df['Age'].apply(lambda x: 'Adult' if x >= 18 else 'Minor')
    
    # Example: Filter rows where 'Age' is greater than 30
    # df_filtered = df[df["Age"] > 30]

    # Example: Group by 'AgeGroup' and calculate the mean age
    df_grouped = df.groupby("AgeGroup")["Age"].mean().reset_index()
    resultsJSON = json.dumps({"values": df_grouped.to_json(orient='records')})          
    return resultsJSON


import numpy as np
@app.function("transform_data")
def transform_data(data: dict )-> str:
    # Extract the items from the input data
    items = data['data']['items']
    # Convert the 2D list to a numpy array
    np_data = np.array(items)

    # Normalize the data (scale values to range [0, 1])
    min_vals = np.min(np_data, axis=0)
    max_vals = np.max(np_data, axis=0)
    normalized_data = (np_data - min_vals) / (max_vals - min_vals)

    # Calculate the mean of each column
    column_means = np.mean(np_data, axis=0)

    return f"Normalized Data: {normalized_data} and Column Means: {column_means}"



@app.fabric_item_input(argName="mylakehouse", alias="lakehousebronze")
@app.function("write_csv_file_in_lakehouse")
def write_csv_file_in_lakehouse(mylakehouse: udf.FabricSqlConnection)-> str:
    data = [(1,"John Smith", 31), (2,"Kayla Jones", 33)]
    csvFileName = "Employees" + str(round(datetime.datetime.now().timestamp())) + ".csv"
       
    # Convert the data to a DataFrame
    df = pd.DataFrame(data, columns=['ID','EmpName', 'DepID'])
    # Write the DataFrame to a CSV file
    df.to_csv(csvFileName, index=False)
       
    # Upload the CSV file to the Lakehouse
    connection = mylakehouse.connectToFiles()
    csvFile = connection.get_file_client(csvFileName)  
    with open(csvFileName, 'r') as file:
        csvFile.upload_data(file.read(), overwrite=True)

    csvFile.close()
    connection.close()
    return f"File {csvFileName} was written to the Lakehouse. Open the Lakehouse in https://app.fabric.microsoft.com to view the files"


    
@app.fabric_item_input(argName="myLakehouse", alias="lakehousebronze")
@app.function("read_csv_from_lakehouse")
def read_csv_from_lakehouse(myLakehouse: udf.FabricLakehouseClient, csvFileName: str) -> str:

    # Connect to the Lakehouse
    connection = myLakehouse.connectToFiles()   

    # Download the CSV file from the Lakehouse
    csvFile = connection.get_file_client(csvFileName)
    downloadFile=csvFile.download_file()
    csvData = downloadFile.readall()
    
    # Read the CSV data into a pandas DataFrame
    from io import StringIO
    df = pd.read_csv(StringIO(csvData.decode('utf-8')))

    # Display the DataFrame    
    result="" 
    for index, row in df.iterrows():
        result=result + "["+ (",".join([str(item) for item in row]))+"]"
    
    # Close the connection
    csvFile.close()
    connection.close()

    return f"CSV file read successfully.{result}"
