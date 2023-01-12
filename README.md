# Import_csv_to_db_postgre
 Automate importing csv to AWS RDS Postgre Database

 This is python file to easily convert all csv files in the working directory, create a dataset folder, and clean it so it's ready for import to db using SQL.
 
 This is achieved by a following along StrataScratch https://www.youtube.com/watch?v=N0aHeKyNEto, where some of the functions were modified so it worked for my case. Reference to StrataScratch for details. 

Steps to execute this file:
1. Download this folder
2. Edit the main.py file to update the following variables according to your RDS database
    database = "postgres"
    user = "weidb"
    password = "xxx"
    host = "database-1.c4xtj4naopjn.us-east-1.rds.amazonaws.com"
    port = '5432'
3. Put your database that you want to import in the same directory as this folder
4. Run main.py file
5. Happy querying!