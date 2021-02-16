"""
Project Title: Microblogging
File Name: Timelines.py
Author: Venkata Pranathi Immaneni
Date: 3rd Oct 2020
Email: ivpranathi@csu.fullerton.edu

"""


import flask
from flask import request, jsonify, g
import sqlite3
import click
import datetime
import time 

from werkzeug.security import generate_password_hash, check_password_hash



app = flask.Flask(__name__)
app.config.from_envvar('APP_CONFIG')

#get the database connection
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(app.config['DATABASE'])
        db.row_factory = make_dicts
    return db

#Used to query db and retrieve the contents from the database
def query_db(query, args=(), one=False):
	cur = get_db().execute(query, args)
	retrieve = cur.fetchall()
	cur.close()
	return (retrieve[0] if retrieve else None) if one else retrieve

#Used to insert data to database	
def inserttodb(query, args=()):
	dbConn = get_db()
	cur = dbConn.cursor()
	cur.execute(query, args)
	dbConn.commit()
	
#Used to close DB connection
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()
        
#Enumerating each row
def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value)
                for idx, value in enumerate(row))


#Def jsonResponse returns the parameters in JSON format.
def jsonResponse(statusCode, message):

	return flask.jsonify(ContentLanguage="en-US", ContentType = "application/json", StatusCode=statusCode, Message=message)
        
#Define postTweet enables an user to post the tweet - in the form of text, which is visible to his followers
#This is of POST method - which takes two parameters in JSON format.
@app.route('/v1/postTweet', methods=['POST'])
def postTweet():
	#Checks whether the content type of the request params is in JSON format, else return an error message.
	if request.headers['Content-Type'] != 'application/json':
		return jsonResponse(400,"Bad Request. Content type should be json")
	
	#Retrieves username and post text from the POST request
	username = request.json.get('username')
	post = request.json.get('post')
	
	#Checks whether username and post are NIL or not. If they are NIL, return error message
	if username is None or post is None:
		return jsonResponse(400,"Missing Username or Post")

	#Checks whether user exists in database
	findUser = query_db('Select * from users where username = ?',
			 [username], one = True)
			 
	#If user does not exists, send error response
	if findUser is None:
		return jsonResponse(400,"User Not Found")

	#Insert the username, content of the post and timestamp into posts table.
	query = "INSERT INTO posts(author, postContent, postTimestamp) VALUES (?,?, ?)"
	inserttodb(query, [username, post, datetime.datetime.now()])
	return jsonResponse(200,"Tweet Posted Successfully")


#def getUserTimeline is used to retrieve recent 25 posts from the user. This is a GET Method
@app.route('/v1/userTimeline/<username>', methods=['GET'])
def getUserTimeline(username):

	#Checks whether user exists in database
	findUser = query_db('Select * from users where username = ?',
			 [username], one = True)
			 
	#If user does not exists, send error response
	if findUser is None:
		return jsonResponse(400,"User Not Found")
		
	#Retrieves the posts of the given user from the database
	getUserPosts = query_db('Select postContent from posts where author = ? ORDER BY postTimestamp desc LIMIT 25', [username], one=False)
	
	#If there posts are not found, error message is sent as response
	if (getUserPosts):
		return jsonResponse(200, getUserPosts)

	return jsonResponse(400, "Posts from the user Not Found")

#def getPublicTimeline is used to retrieve recent 25 posts from all the users. This is a GET Method
@app.route('/v1/publicTimeline', methods=['GET'])
def getPublicTimeline():

	#Retrives all the recent posts 25 from posts table
	getAllPosts = query_db('Select * from posts ORDER BY postTimestamp desc LIMIT 25', one=False)
	
	#If there posts are not found, error message is sent as response
	if getAllPosts is None:
		return jsonResponse(400, "Posts Not Found")
	return jsonResponse(200, getAllPosts)
	
	
#def getHomeTimeline is used to retrieve recent 25 posts from all the users, that th. This is a GET Method
@app.route('/v1/homeTimeline/<username>', methods=['GET'])
def getHomeTimeline(username):

	#Checks whether user exists in database
	findUser = query_db('Select * from users where username = ?',
			 [username], one = True)
			 
	#If user does not exists, send error response
	if findUser is None:
		return jsonResponse(400,"User Not Found")
	
	#Retrieves all the recent 25 posts from the users that given username is following
	getAllPosts = query_db('Select * from posts p INNER JOIN followers f where ((f.username = ?) AND (f.followerUsername = p.author)) ORDER BY postTimestamp desc LIMIT 25', [username], one=False)
	
	#If there posts are not found, error message is sent as response
	if (getAllPosts):
		return jsonResponse(200, getAllPosts)
	return jsonResponse(400, "Posts Not Found")
	
	
	
		
	
		

	

