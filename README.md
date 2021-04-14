# Cricket Player Information

This Project/Repository is a part of Cricket Player Information Serve project.

The Project involves 2 steps
1. Scrap the player information from an website and store it on Structured database.
2. Serve the data as per request.

The main aim of this project/repository is to scrape the cricket player information from a website and store it in Azure Cosmos database.

The data is scrapped from <a href="http://www.howstat.com/cricket/home.asp" rel="nofollow" target="_blank">Howstat.com</a>.

Azure functions is used to trigger an event everyday at 3:00pm to fetch and store data. The existing documents are dropped and new data's are updated.

Python is used to parse the asp website. Mongodb API service is used to access cosmos db.

### List of Services/Languages used:
1. Python - To scrape and connect to database
2. Threading in python 
3. Azure functions - To trigger python function to perform store and scraping task
4. Cosmos DB 
5. Mongodb API - To perform CRUD operations on Cosmos Database