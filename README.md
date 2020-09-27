# Guide to follow my solution

### 1. Step - Get the code
You need to have python 3 installed. To check it:
```sh
python3 --version
```
Enter in a folder with terminal or Git bash for windows, then :
```sh
git clone git@github.com:anasardico/VM-Data-Engineer.git
```
```sh
git pull
```
### 2. Step - Create and Activate environment
Unix :
```sh
python3 -m venv venv
```
```sh
venv\Scripts\Activate.ps1
```
Windows :
```sh
python3 -m venv venv
```
```sh
venv\Scripts\activate.bat
```
### 3. Installation and usage

Install requirements:
```sh
pip install -r requirements.txt
```

### 4. Ingest the three tables into a Postgres database
You'll need a PostgreSQL database.
Inside ETL_VoiceMod please, change for your PostgreSQL password. 
Then, in terminal/cmd:
```sh
python etl_voice_mod.py
```

### 5. Run Aggregation Daily Job
For this part you need a MongoDB database for your operating system.
Please open the aggr_to_NoSQL Script and insert your password.

Unix:
Open the terminal. Then:
```sh
which python
```
This gives you a path. Example: /usr/bin/python.
Then,
```sh
crontab -e
```
This open the crontab. Write the next statement inside:
```sh
30 23 * * * /usr/bin/python project_folder/aggr_to_no_sql_db.py
```
Windows:

In windows the tool to use is Task Scheduler. 
Create a folder in the library.
Then, create a new daily task where in the actions you put :

- program: python3
- arguments: the path for aggr_to_no_sql_db.py Script
 

### 6. Deactivate the environment

```sh
deactivate
```
#### Notes

The file logic.txt explain my thoughts in the code developed here. 

Free to explain me better.

Thanks for having me!