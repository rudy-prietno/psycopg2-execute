import time
import psycopg2
import psycopg2.extras

TABLE_NAME = 'TestTable'


def measure_time(func):
    def time_it(*args, **kwargs):
        time_started = time.time()
        func(*args, **kwargs)
        time_elapsed = time.time()
        print("""{execute} running time is {sec} seconds for inserting {rows} rows.
              """.format(execute=func.__name__,
                         sec=round(time_elapsed - time_started, 4),
                         rows=len(kwargs.get('values'))))

    return time_it


class PsycopgTest():

    def __init__(self, num_rows):
        self.num_rows = num_rows

    def create_dummy_data(self):
        values = []
        for i in range(self.num_rows):
            values.append((i + 1, 'test'))
        return values

    def connect(self):
        conn_string = "host={0} port={1} user={2} dbname={3} password={4}".format('localhost',
                                                                                  '5432',
                                                                                  'postgres',
                                                                                  'postgres', 
                                                                                  'secrect')
        self.connection = psycopg2.connect(conn_string)
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY, NAME text)".format(table=TABLE_NAME))
        self.connection.commit()

    def truncate_table(self):
        self.cursor.execute("TRUNCATE TABLE {table} RESTART IDENTITY".format(table=TABLE_NAME))
        self.connection.commit()
        
    @measure_time
    def method_execute(self, values):
        """Loop over the dataset and insert every row separately"""
        for value in values:
            self.cursor.execute("INSERT INTO {table} VALUES (%s, %s)".format(table=TABLE_NAME), value)
        self.connection.commit()

    @measure_time
    def method_execute_many(self, values):
        self.cursor.executemany("INSERT INTO {table} VALUES (%s, %s)".format(table=TABLE_NAME), values)
        self.connection.commit()

    @measure_time
    def method_execute_batch(self, values):
        psycopg2.extras.execute_batch(self.cursor, "INSERT INTO {table} VALUES (%s, %s)".format(table=TABLE_NAME),
                                      values)
        self.connection.commit()

    @measure_time
    def method_string_building(self, values):
        argument_string = ",".join("('%s', '%s')" % (x, y) for (x, y) in values)
        self.cursor.execute("INSERT INTO {table} VALUES".format(table=TABLE_NAME) + argument_string)
        self.connection.commit()

    @measure_time
    def method_string_building_test(self, values):
        length_list = len(values[0])* ("%s",)
        argument_string = ",".join('{}'.format(length_list) % (x, y) for (x, y) in values)
        self.cursor.execute("INSERT INTO {table} VALUES".format(table=TABLE_NAME) + argument_string)
        self.connection.commit()
        
    @measure_time
    def method_string_execute_many(self, values):
        argument_string = ','.join(['%s'] * len(values[0]))
        query= """INSERT INTO {} VALUES ({})""".format(TABLE_NAME, argument_string)
        self.cursor.executemany(query, values)
        self.connection.commit()

def main():
    psyco = PsycopgTest(10000)
    psyco.connect()
    values = psyco.create_dummy_data()
    psyco.create_table()
    psyco.truncate_table()
    # psyco.method_execute(values=values)
    # display((values))
    # psyco.method_execute_many(values=values)
    # psyco.method_execute_batch(values=values)
    # psyco.method_string_building(values=values)
    # psyco.method_string_execute_many(values=values)
    psyco.method_string_building_test(values=values)


if __name__ == '__main__':
    main()
