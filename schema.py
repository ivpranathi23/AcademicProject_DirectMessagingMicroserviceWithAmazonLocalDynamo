"""
Project Title: Microblogging
File Name: app.py
Author: Venkata Pranathi Immaneni
Modified Date: 1st Jan 2021
Email: ivpranathi@csu.fullerton.edu

"""

import flask
from flask import request, jsonify, g
import sqlite3
import click
from werkzeug.security import generate_password_hash, check_password_hash
import boto3
from botocore.exceptions import ClientError

app = flask.Flask(__name__)
app.config.from_envvar('APP_CONFIG')

#creating direct message table. Here message id is the partition key and timestamp is the sort key
def create_DirectMessage_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    isTableExists = dynamodb.Table('DirectMessage')
    #if table already exists delete the table and initialise again
    #if isTableExists is not None:
    	#isTableExists.delete()

    table = dynamodb.create_table(
        TableName='DirectMessage',
        KeySchema=[
            {
                'AttributeName': 'MessageId',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'MessageId',
                'AttributeType': 'N'
            },
            {
            	'AttributeName': 'to_Username',
                'AttributeType': 'S'
            },
            {
            	'AttributeName': 'timestamp',
                'AttributeType': 'S'
            }

        ],
        GlobalSecondaryIndexes=[
        	{
        		'IndexName': 'UsernameIndex',
        		'KeySchema': [
        			{
        				'AttributeName': 'to_Username',
        				'KeyType': 'HASH'
        			},
        			{
        				'AttributeName': 'timestamp',
        				'KeyType': 'RANGE'
        			}
        		],
        		'Projection': {
        			'ProjectionType': "ALL"
        		},
        		'ProvisionedThroughput': {
        			'ReadCapacityUnits': 1,
        			'WriteCapacityUnits': 1
        		}
        	},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table
    
#Get the database connection
def get_db():
    dm_table = create_DirectMessage_table()
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(app.config['DATABASE'])
    return db
    
#Def init_db is used to initialise database
@app.cli.command('init')
def init_db():
    with app.app_context():
    	print("in schema initialisation")
    	dm_table = create_DirectMessage_table()
    	db = get_db()
    	with app.open_resource('schema.sql', mode='r') as f:
    		db.cursor().executescript(f.read())
    	db.commit()
        
#Def teardown is Used to close DB connection
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

