### Importation des packages
import pandas as pd
import boto
import boto3


# Création du curseur

dynamodb = boto3.resource('dynamodb', aws_access_key_id='AKIAJWYEGDPGOV4B2KKQ', aws_secret_access_key= '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', region_name='eu-west-3')

## Création table Langue réalisateur

table = dynamodb.create_table(
    TableName='DirectorLanguage',
    KeySchema=[
        {
            'AttributeName': 'Director',
            'KeyType': 'HASH'  #Partition key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'Director',
            'AttributeType': 'S'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

print("Table status:", table.table_status)

## Creation Table Awards
table = dynamodb.create_table(
    TableName='Awards',
    KeySchema=[
        {
            'AttributeName': 'Director',
            'KeyType': 'HASH'  #Partition key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'Director',
            'AttributeType': 'S'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

## Creation Table ID films

table = dynamodb.create_table(
    TableName='FilmID',
    KeySchema=[
        {
            'AttributeName': 'ID',
            'KeyType': 'HASH'  #Partition key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'ID',
            'AttributeType': 'N'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

## Creation Table casting des films

table = dynamodb.create_table(
    TableName='Casting',
    KeySchema=[
        {
            'AttributeName': 'FilmID',
            'KeyType': 'HASH'  #Partition key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'FilmID',
            'AttributeType': 'N'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

## Creation Table Présentation film
table = dynamodb.create_table(
    TableName='FilmDescription',
    KeySchema=[
        {
            'AttributeName': 'ID',
            'KeyType': 'HASH'  #Partition key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'ID',
            'AttributeType': 'N'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)



## Creation table des Awards par catégories (et cérémonies)
table = dynamodb.create_table(
    TableName='AwardsByCategory',
    KeySchema=[
        {
            'AttributeName': 'Director',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'Category',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'Director',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Category',
            'AttributeType': 'S'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 3,
        'WriteCapacityUnits': 3
    }
)



### Récupération d'items

table = dynamodb.Table('FilmID')

response = table.get_item(
    Key={
        'ID': 3
    }
)
item = response['Item']
print(item)




