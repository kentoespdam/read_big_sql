# Dependencies
`pip install mysql-connector-python tqdm`

## Usage
>**Restore Full Database** \
`python main.py backup.sql restore \
  --db-host localhost \
  --db-user root \
  --db-password password123 \
  --db-name mydatabase \
  --create-database \
  --batch-size 2000`

>**Restore Specific Tables dari Report** \
`python main.py dump.sql restore \
  --from-report \
  --report analysis_report.json \
  --tables users,products,orders \
  --db-host localhost \
  --db-user root \
  --db-name mydb \
  --drop-table`

>**Restore dengan Config File**
>> Buat config file dulu \
> python database_config.py
> 
>> Edit database.ini dengan credentials yang benar \
> Kemudian restore
>
> `python main.py backup.sql restore --db-host localhost --db-user root`

>**Dengan Skip Errors** \
> `python main.py large_dump.sql restore \
  --db-host localhost \
  --db-user root \
  --db-name mydb \
  --skip-errors \
  --verbose`

>**Create Backup sebelum Restore**
>> Backup database existing \
> `python backup_utils.py --backup --db-name mydb`
>
>> Kemudian restore \
> `python main.py new_data.sql restore \
  --db-host localhost \
  --db-user root \
  --db-name mydb`