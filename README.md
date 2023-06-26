# Capstone-Projects

YouTube Data Harvesting and Warehousing

**Description**
Welcome to our project! This is a Youtube data harvesting App made in Python and the Streamlit framework. It allows users to quickly and easily fetch Youtube channel details by giving Youtube channel IDs.
Also it allows users to store channel data in MongoDB and MySQL and perform analysis. 

**Contributing**
We welcome any and all contributions! Here are some ways you can get started:
1.	Report bugs: If you encounter any bugs, please let us know. Open up an issue and let us know the problem.
2.	Contribute code: If you are a developer and want to contribute, follow the instructions below to get started!
3.	Suggestions: If you don't want to code but have some awesome ideas, open up an issue explaining some updates or imporvements you would like to see!
4.	Documentation: If you see the need for some additional documentation, feel free to add some!

   
**Pre-Requirements**
**Changes need to be made in the cod**e:
1.	In MongoDB Atlas connection credentials section of the code, update credentials of the database.
2.	In connect_mysql function, update MYSQL credentials ,(mysql_user and mysql_password)
3.	In youtube_api function, update, youtube API key. If not created and API key refer https://developers.google.com/youtube/v3/getting-started

**Python Set-up**
1.	Make sure, following libraries are installed in Python, If not install using below command
pip install <library name>
•	streamlit 
•	pymongo
•	google-python-api-client
•	pandas
•	mysql-connector-python

WORKFLOW
Application Interface
 
1.	Enter Channel ID/s (comma separated list , maximum 10 values)
 
•	Input from here will be sent to youtube API to request for channel information

2.	Click on Retrieve Channel Data button
 
•	Relevant channel details will displayed in the form of a table.(Scroll to view the complete details)

3.	Click on Move to MongoDB Atlas button to move data to MongoDB Atlas.
 
This action will move the data to Mongo DB and store it in form of embedded document.
 

4.	Click on Move data to My SQL to move data to SQL tables(Channel,Playlist,Video and Comment)
 
 

5.	From Query selection drop down, select what result needs to be displayed on screen.You Can select multiple options.

 


Sample Outputs:
 


 
