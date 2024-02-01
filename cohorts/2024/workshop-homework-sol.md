# Workshop Homework Solution
## Author: Jonah Oliver
1. 8.382332347441762
2. 3.605551275463989
3. 353
4. 266

# Solution Code
```python
# Question 1 Solution
limit = 13
generator = square_root_generator(limit)
iterations = 0

for index, sqrt_value in enumerate(generator):
  iterations += 1
  if index == 12:
    print(f'iterations: {iterations}')
    print(f'index: {index}')
    print(f'sqr root: {sqrt_value}')
# 3.605551275463989

limit = 5
generator = square_root_generator(limit)

total_sum = sum(sqrt_value for sqrt_value in generator)
print(f'total: {total_sum}') 
# 8.382332347441762

# Question 2 Solution

age_sum = sum(person['Age'] for person in people_1())
print(age_sum) 
# 140


# Question 3 & 4 Solution
import dlt
import duckdb

generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')

# to show available methods, run:
# dir(generators_pipeline)

pipeline_info = generators_pipeline.run(people_1(), table_name='people_q2', write_disposition='replace')

pipeline_info = generators_pipeline.run(people_2(), table_name='people_q2', write_disposition='append')

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print(f"SET search_path = '{generators_pipeline.dataset_name}'")
# print('Loaded tables: ')
# display(conn.sql("show tables"))

conn.sql("SELECT * FROM people_q2")
#conn.sql("SELECT SUM(age) from people")

pipeline_info = generators_pipeline.run(people_1(), table_name='people_q3', write_disposition='replace', primary_key='id')

pipeline_info = generators_pipeline.run(people_2(), table_name='people_q3', write_disposition='merge', primary_key='id')

print('Loaded tables: ')
display(conn.sql("show tables"))

print('Question 3 Result Table')
display(conn.sql("SELECT * FROM people_q3 ORDER BY id"))
print("Sum of all ages from Question 3 Table")
display(conn.sql("SELECT SUM(age) from people_q3"))
```
