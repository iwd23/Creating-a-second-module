import pandas as pd
from psycopg2 import connect

# Чтение файла Excel
df = pd.read_excel('/home/iwd/Documents/module2/info.xlsx', header=0)

# Список имён столбцов для проверки
column_names = ['StudFIO', 'DocID', 'Permission-ID', 'End-Of-Life-Pass', 'Creation', 'Course', 'Group', 'Fak']

# Проверка наличия всех имён столбцов из списка в DataFrame
for name in column_names:
    if name not in df.columns:
        print(f'{name} не найден в файле.')
    else:
        pass  # Имя найдено
    
# def check_row(row):
#     if row.any():
#         return True
#     else:
#         return False

# Проверяем, есть ли записи в каждой строке
for index, row in df.iterrows():
    if row.any():  # Если в строке есть хотя бы одна запись
        print(row)  # Выводим строку
    else:  # Иначе
        print("Строки закончились")
        break  # Прерываем цикл

# Создайте соединение с базой данных
conn = connect(host='db', database='db', user='admin', password='admin')

# Создание курсора
cur = conn.cursor()

# # Запустите цикл по строкам
# for index, row in df.iterrows():
#     if check_row(row):
#         # Запишите данные в базу
#         cursor = conn.cursor()
#         sql = "INSERT INTO table (column1, column2, column3, column4, column5, column6, column7, column8) VALUES (%s, %s)"
#         values = (row['column1'], row['column2'], row['column3'], row['column4'], row['column5'], row['column6'], row['column7'], row['column8'])
#         cursor.execute(sql, values)

# Запись данных в базу данных
for index, row in df.iterrows():
    cur.execute("INSERT INTO mytable (column1, column2, column3, column4, column5, column6, column7, column8) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (row['column1'], row['column2'], row['column3'], row['column4'], row['column5'], row['column6'], row['column7'], row['column8']))

# Commit изменений
conn.commit()

# Закрытие курсора и соединения
cur.close()
conn.close()

# Формирование ответа в формате JSON
json_data = df.to_json(orient='records')
print(json_data)

# # Вывод содержимого файла
# print(df)
# print(index)
