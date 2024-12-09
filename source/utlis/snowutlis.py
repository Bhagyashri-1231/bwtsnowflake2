import snowflake.connector

class snowflakeconn:
    def __init__(self, account, user, password, warehouse, database, schema, role):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role

    def connect(self):
        self.connection = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def close_connection(self):
        self.cursor.close()
        self.connection.close()

# Connection details
account = "CRUERCV-FC11347"
user = "BHAGYASHREE"
password = "Shreer@m123"
warehouse = "COMPUTE_WH"
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCH_SF1"
role = "SYSADMIN"

# Create an instance of the snowflakeconn class
SF_CONNECTOR = snowflakeconn(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse,
    database=database,
    schema=schema,
    role=role
)

# Establish the connection
SF_CONNECTOR.connect()

# Execute query
query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER LIMIT 10"
result = SF_CONNECTOR.execute_query(query)
print(result)

# Close connection
SF_CONNECTOR.close_connection()
