def getSIDRATable(url):
    #Importing required libraries
    import pandas as pd
    import requests
    
    #Getting data from API
    response = requests.get(url).json()
    
    #Converting the response to dataframe
    df = pd.DataFrame(response)
    df.columns = df.iloc[0]
    df.drop(0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    #Standardizing columns names
    columnList = []
    for i in range(len(df.columns)):
        tempString = df.columns[i].replace(" ", "_")
        tempString = tempString.lower()
        tempString = tempString.replace("(código)","cod")
        tempString = tempString.replace("á","a")
        tempString = tempString.replace("é","e")
        tempString = tempString.replace("í","i")
        tempString = tempString.replace("ó","o")
        tempString = tempString.replace("ú","u")
        tempString = tempString.replace("ã","a")
        tempString = tempString.replace("õ","o")
        tempString = tempString.replace("â","a")
        tempString = tempString.replace("ê","e")
        tempString = tempString.replace("î","i")
        tempString = tempString.replace("ô","o")
        tempString = tempString.replace("û","u")
        tempString = tempString.replace("ç","c")
        tempString = tempString.replace("(","")
        tempString = tempString.replace(")","")
        columnList.append(tempString)
    df.columns = columnList
    
    return df

def getBACENTable(url):
    #Importing required libraries
    import pandas as pd
    import requests
    
    #Getting data from API
    response = requests.get(url).json()
    
    #Converting the response to dataframe
    df = pd.DataFrame(response)
    
    #Standardizing columns names
    columnList = []
    for i in range(len(df.columns)):
        tempString = df.columns[i].replace(" ", "_")
        tempString = tempString.lower()
        tempString = tempString.replace("(código)","cod")
        tempString = tempString.replace("á","a")
        tempString = tempString.replace("é","e")
        tempString = tempString.replace("í","i")
        tempString = tempString.replace("ó","o")
        tempString = tempString.replace("ú","u")
        tempString = tempString.replace("ã","a")
        tempString = tempString.replace("õ","o")
        tempString = tempString.replace("â","a")
        tempString = tempString.replace("ê","e")
        tempString = tempString.replace("î","i")
        tempString = tempString.replace("ô","o")
        tempString = tempString.replace("û","u")
        tempString = tempString.replace("ç","c")
        tempString = tempString.replace("(","")
        tempString = tempString.replace(")","")
        columnList.append(tempString)
    df.columns = columnList
    
    return df

def dataframeToPostgreTable(user, pwd, host, port, dbname, tbname, schema, dataframe):
    from sqlalchemy import create_engine
    conn_string = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(user, pwd, host, port, dbname)
    engine = create_engine(conn_string)
    tbname = tbname.lower() #For compatibility issues
    dataframe.to_sql(tbname, engine, schema=schema, if_exists='replace', index=False)
    print("{} - Table created with success.".format(tbname))

