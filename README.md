
# YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

App Link: https://youtubeproject123.streamlit.app/

To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

The application should have the following features:
  * Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
 * Option to store the data in a MongoDB database as a data lake.
 * Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
 * Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

Steps done are given below:
* Set up a Streamlit app - I used simple codes in streamlit app.
* Connect to the YouTube API - I generated a youtube API key and extracted the data from Youtube using Youtube API.
* Store data in a MongoDB data lake - I stored the data in MongoDB database by importing and connecting it to the host using MongoDB Atlas or MongoDB Compass.
* Migrate data to a SQL data warehouse - I migrated the data stored in the MongoDB to SQL database for creating tables and displaying it by connecting to the SQL host using MySQL or PostgresSQL or SQLite.
* Query the SQL data warehouse - Searching the required data in SQL with the questions and joining the tables is done here.
* Display data in the Streamlit app - Once all the data extraction, and warehousing is completed we can display the data in Streamlit app by storing the coded file as python file with extension .py which will be recognized by Streamlit app.

