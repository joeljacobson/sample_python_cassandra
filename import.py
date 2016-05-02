import json
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement

cluster = Cluster(['172.17.0.2'])
session = cluster.connect()

session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS customer_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
        """
)

session.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_data.customers ( customer_id int PRIMARY KEY, first_name text, last_name text, email text );
        """
)


json_data = {}
with open('data.json') as json_file:
    json_data = json.load(json_file)

batch = BatchStatement()

for data in json_data:
        customer_id = data['id']
        first_name = data['first_name']
        last_name = data['last_name']
        email = data['email']
        batch.add(SimpleStatement("INSERT INTO customer_data.customers (customer_id, first_name, last_name, email) VALUES (%s, %s, %s, %s)"), (customer_id, first_name, last_name, email))

session.execute(batch)


