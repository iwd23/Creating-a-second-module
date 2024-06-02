import pandas as pd
from psycopg2 import connect

# Чтение файла Excel
df = pd.read_excel('/home/iwd/Documents/module2/info.xlsx', header=0, index_col=0)

if 'StudFIO' in df.columns and 'DocID' in df.columns and \
   'Permission ID' in df.columns and 'End-Of-Life Pass' in df.columns and \
   'Creation' in df.columns and 'Course' in df.columns and \
   'Group' in df.columns and 'Fak' in df.columns:
    print("Все колонки присутствуют")
else:
    raise ValueError("Не все колонки присутствуют!")

for col in ['StudFIO', 'DocID', 'Permission ID', 'End-Of-Life Pass',
            'Creation', 'Course', 'Group', 'Fak']:
    if not (df[col].dtype == 'string' or df[col].dtype == int):
        raise TypeError(f"Колонка {col} имеет неправильный тип данных!")
    
def check_row(row):
    if row.any():
        return True
    else:
        return False
    
# Создайте соединение с базой данных
conn = connect(host='localhost', database='mydb', user='postgres', password='password')

# Запустите цикл по строкам
for index, row in df.iterrows():
    if check_row(row):
        # Запишите данные в базу
        cursor = conn.cursor()
        sql = "INSERT INTO table (column1, column2) VALUES (%s, %s)"
        values = (row['column1'], row['column2'])
        cursor.execute(sql, values)

# Закройте соединение
cursor.close()
conn.close()

# Вывод содержимого файла
print(df)
