# Importation des données
import pandas as pd
data = pd.read_csv('C:/Users/Zoé/Documents/ACO/M2/S9/BigData/archive/MostCommonLanguageByDirector.csv')
print(data.head(3))
print(data.columns)

import boto3

# Création du curseur

dynamodb = boto3.resource('dynamodb', aws_access_key_id='AKIAJWYEGDPGOV4B2KKQ', aws_secret_access_key= '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', region_name='eu-west-3')

# Création de la table

table = dynamodb.create_table(
    TableName='DirectorLanguage',
    KeySchema=[
        {
            'AttributeName': 'Director',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'Language',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'Director',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Language',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

print("Table status:", table.table_status)

# Ajouter les données à la table

for k in range(len(data) - 1):


    table = dynamodb.Table('DirectorLanguage')

    trans = {}

    trans['Director'] = data['director_name'][k]
    trans['Language'] = data['original_language'][k]

    table.put_item(Item=trans)








