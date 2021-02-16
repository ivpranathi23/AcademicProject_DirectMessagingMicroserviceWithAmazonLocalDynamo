"""
Project Title: Polyglot Persistence
File Name: app.py
Author: Venkata Pranathi Immaneni
Date: 24th Nov 2020
Email: ivpranathi@csu.fullerton.edu
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb //For starting dynamo service
"""
import flask
from flask import request, jsonify, g
import sqlite3
import uuid
import click
from werkzeug.security import generate_password_hash, check_password_hash
import boto3
from botocore.exceptions import ClientError
import random
import datetime
import time
from boto3.dynamodb.conditions import Key
 
app = flask.Flask(__name__)
app.config.from_envvar('APP_CONFIG')

#get the database connection
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(app.config['DATABASE'])
    return db

#Used to query db and retrieve the contents from the database
def query_db(query, args=(), one=False):
	cur = get_db().execute(query, args)
	retrieve = cur.fetchall()
	cur.close()
	return (retrieve[0] if retrieve else None) if one else retrieve
  
#Def jsonResponse returns the parameters in JSON format.
def jsonResponse(statusCode, message):

	return flask.jsonify(ContentLanguage="en-US", ContentType = "application/json", StatusCode=statusCode, Message=message),statusCode

'''	
sendDirectMessage api is used to send the direct message to a particular user. It is a POST Method.
Pass the parameter in JSON Format.


to_username = Message receipient name
from_username = Message sender name
message = text message that we want to send
'''

@app.route('/v1/sendDirectMessage', methods=['POST'])
def sendDirectMessage():
	#Checks whether the content type of the request params is in JSON format, else return an error message.
	if request.headers['Content-Type'] != 'application/json':
		return jsonResponse(400,"Bad Request. Content type should be json")

	#Retrieves username, password, text and  quick replies from the POST request
	to_username = request.json.get('to_username')
	from_username = request.json.get('from_username')
	text = request.json.get('message')
	quickReplies = request.json.get('quickReplies')
	inReplyTo = request.json.get('inReplyTo')

	#Checks whether all the required parameters are passed else returns the error message.
	if to_username is None or from_username is None:
		return jsonResponse(400,"Missing required fields")
		
	#check whether the to_username is already present in the users database
	find_To_User = query_db('Select * from users where username = ?',
			 [to_username], one = True)
	if find_To_User is None:
		return jsonResponse(409,"The user to whom you are sending message does not Exists")
		
	#check whether the from_username is already present in the users database
	find_From_User = query_db('Select * from users where username = ?',
			 [from_username], one = True)
	if find_From_User is None:
		return jsonResponse(409,"The from user does not Exists")
	
	#generate current timestamp
	dateTimeObj = datetime.datetime.now().isoformat()
	
	#message id - using uuid4 for generating message id
	generateMsgId = uuid.uuid4().int>>64
	
	#Check whether inReplyTo Message Is Valid
	if inReplyTo is not None:
		isMessageIdExistsRes = get_messages(inReplyTo)
		if len(isMessageIdExistsRes['Items']) <= 0:
    			return flask.jsonify(StatusCode=404, Message="In Reply To - Message id not found", ContentType='application/json'),404
		
	response = put_vals(generateMsgId, to_username, from_username, dateTimeObj, text, None, quickReplies, inReplyTo)
		
	if response['ResponseMetadata']['HTTPStatusCode'] is 200:
		return flask.jsonify(StatusCode=200, Message="Message sent successfully", messageId=generateMsgId, ContentType='application/json')
	else:
		return response


#This is a helper function for send Direct Message Api. It connects to dynamo and retrives the table and inserts the message into the DirectMessage Table
def put_vals(messageId, to, fromuser, timestamp, text="", dynamodb=None, quickReplies=None, inReplyTo=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('DirectMessage')
    response = table.put_item(
       Item={
            'MessageId': messageId,
            'to_Username': to,
            'from_username': fromuser,
            'timestamp': timestamp,
            'text': text,
            'replies': [],
            'quickReplies': quickReplies,
            'inReplyTo': inReplyTo
            }
    )
    return response 


'''	
replyDirectMessage api is used to reply to the direct message to a particular user - by specifying the message id. It is a POST Method.
Pass the parameter in JSON Format.


messageId = Message id to which we reply
reply = text message that we want to reply.
'''

@app.route('/v1/replyDirectMessage', methods=['POST'])
def replyDirectMessage():
	if request.headers['Content-Type'] != 'application/json':
		return jsonResponse(400,"Bad Request. Content type should be json")

	messageid = request.json.get('messageId')
	replyText = request.json.get('reply')
	quickReplyId = request.json.get('quickReplyId')
	
	if (messageid is None) or (replyText is None and quickReplyId is None):
		return jsonResponse(400,"Missing required parameters")
	return update_messageReplies(messageid, replyText, quickReplyId, None)
	
#Retrives the existing replies of a particular message id and updates by appending the new replies to the existing ones.
def update_messageReplies(messageid, replyText=None, quickRepliesId=None, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('DirectMessage')
    res = get_messages(messageid, None)
    #check if elements with message id are found or not
    if len(res['Items']) <= 0:
    	return flask.jsonify(StatusCode=404, Message="Message id not found", ContentType='application/json'),404
    
    replies = res['Items'][0]['replies']  #Existing replies of message id
    
     #Appends the new reply
    if replyText is not None:
    	replies.append(replyText)
    
    if quickRepliesId is not None:
    	keys = res['Items'][0]['quickReplies']
    	if quickRepliesId not in keys:
    		return flask.jsonify(StatusCode=404, Message="Quick reply id not found. Please enter a valid quick reply id", ContentType='application/json'),404
    		
    	getQuickReply = res['Items'][0]['quickReplies'][quickRepliesId]    		
    	replies.append(getQuickReply)

    response = table.update_item(
        Key={'MessageId': messageid},
        UpdateExpression="set replies=:r",
        ExpressionAttributeValues={
        ':r': replies
        },
        ReturnValues="UPDATED_NEW"
        )
    
    if response['ResponseMetadata']['HTTPStatusCode'] is 200:
    	return jsonResponse(200, 'Reply sent successfully')
    else:
    	return response


#Retrives all the messages of a particular message id. It filters based on the partition key.    
def get_messages(messageid, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('DirectMessage')
    filtering_exp = Key('MessageId').eq(messageid)
    response = table.query(KeyConditionExpression=filtering_exp)
    return response

'''	
listDMFor api is used to list all the direct message sent to particular username. It is a GET Method. Here, username  is the Global secondary index of the table.
Pass the parameter as Query param.


username = username whose direct messages to be retrieved
'''
@app.route('/v1/listDMFor', methods=['GET'])
def listDirectMessage():

	#retrive username. If not passed return error
	username = request.args.get('username')
	if username is None:
		return jsonResponse(400,"Missing Username")
	
	#Checks whether user exists in database
	findUser = query_db('Select * from users where username = ?',
			 [username], one = True)

	#If user does not exists, send error response
	if findUser is None:
		return jsonResponse(400,"User Not Found")
	
	#retrieveDMs method retrieves all the DMs of a particular user
	response = retriveDMs(username, None)
	#check for DM's. If not present return error
	if len(response['Items']) <= 0:
    		return flask.jsonify(StatusCode=404, Message="No Direct messages found for this user", ContentType='application/json')
    		
	dmlist = []
	#Iterate over each dm and retrive only DM text
	for dm in response['Items']:
		dmlist.append(dm['text'])
	return jsonResponse(200, dmlist)

#THis method performs the query to retrive direct message based on the username, which is the global secondary index
def retriveDMs(username, dynamodb=None):
	if not dynamodb:
        	dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
	table = dynamodb.Table('DirectMessage')
	response = table.query(
	IndexName="UsernameIndex",
	KeyConditionExpression=Key('to_Username').eq(username),
	)
	
	return response

'''	
listReplies api is used to list all the replies of a particular message id


messageid = messageid whose replies are be retrieved
'''
@app.route('/v1/listReplies', methods=['GET'])
def listRepliesTo():
	messageId = request.args.get('MessageId')
	if messageId is None:
		return jsonResponse(400,"Missing MessageId - check for case sensitiveness")
	return listingReplies(messageId, None)
	
#This is a helper method to retrive all the replies of a particular message id from Direct Message table
def listingReplies(messageid, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('DirectMessage')
    
    try:
        int(messageid)
    except ValueError:
        return flask.jsonify(StatusCode=404, Message="Message id not Invalid.", ContentType='application/json')
        
    	
    res = get_messages(int(messageid), None)
    if len(res['Items']) <= 0:
    	return flask.jsonify(StatusCode=404, Message="Message id not found", ContentType='application/json')
    
    replies = res['Items'][0]['replies']
    print("*************")
    print(res['Items'])
    return flask.jsonify(replies)

	
	
	

  


	
		
	


