# import pandas as pd
# from psycopg2 import connect
# import json

# file_path = '{{ file_path }}'

# # Проверка наличия файла Excel
# try:
#     df = pd.read_excel(file_path, header=0)
# except FileNotFoundError:
#     error = {'error': 'Файл Excel не найден'}
#     print(json.dumps(error))
#     exit(1)

# # Список имён столбцов для проверки
# column_names = ['StudFIO', 'DocID', 'Permission-ID', 'End-Of-Life-Pass', 'Creation', 'Course', 'Group', 'Fak']

# # Проверка наличия всех имён столбцов из списка в DataFrame
# missing_columns = [name for name in column_names if name not in df.columns]
# if missing_columns:
#     error = {'error': f'Отсутствуют столбцы: {", ".join(missing_columns)}'}
#     print(json.dumps(error))
#     exit(1)

# # Проверяем, есть ли записи в каждой строке
# empty_rows = [index for index, row in df.iterrows() if not row.any()]
# if empty_rows:
#     error = {'error': f'Пустые строки: {", ".join(map(str, empty_rows))}'}
#     print(json.dumps(error))
#     exit(1)

# # Создайте соединение с базой данных
# try:
#     conn = connect(host='db', database='db', user='admin', password='admin')
# except Exception as e:
#     error = {'error': f'Ошибка подключения к базе данных: {str(e)}'}
#     print(json.dumps(error))
#     exit(1)

# # Создание курсора
# cur = conn.cursor()

# # Запись данных в базу данных
# try:
#     for index, row in df.iterrows():
#         cur.execute("INSERT INTO mytable (column1, column2, column3, column4, column5, column6, column7, column8) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
#                     (row['column1'], row['column2'], row['column3'], row['column4'], row['column5'], row['column6'], row['column7'], row['column8']))
#     conn.commit()
# except Exception as e:
#     error = {'error': f'Ошибка записи в базу данных: {str(e)}'}
#     print(json.dumps(error))
#     exit(1)

# # Закрытие курсора и соединения
# cur.close()
# conn.close()

# # Формирование ответа в формате JSON
# json_data = df.to_json(orient='records')
# print(json_data)

import pandas as pd
from psycopg2 import connect
import json

def load_excel_file(file_path):
    
    # Загрузка файла Excel в Pandas DataFrame
    
    try:
        df = pd.read_excel(file_path, header=0)
        return df, None
    except FileNotFoundError:
        error = {'error': 'Файл Excel не найден', 'changed': False}
        return None, error

def validate_column_names(df, column_names):
    
    # Проверка того, что все необходимые имена столбцов присутствуют в DataFrame
    
    missing_columns = [name for name in column_names if name not in df.columns]
    if missing_columns:
        error = {'error': f'Отсутствуют столбцы: {", ".join(missing_columns)}', 'changed': False}
        return False, error
    return True, None

def validate_empty_rows(df):
    
    # Проверка того, что в DataFrame нет пустых строк
    
    empty_rows = [index for index, row in df.iterrows() if not row.any()]
    if empty_rows:
        error = {'error': f'Пустые строки: {", ".join(map(str, empty_rows))}', 'changed': False}
        return False, error
    return True, None

def connect_to_database():
    
    # Установка соединения с базой данных
    
    try:
        conn = connect(host='db', database='db', user='admin', password='admin')
        return conn, None
    except Exception as e:
        error = {'error': f'Ошибка подключения к базе данных: {str(e)}', 'changed': False}
        return None, error

def insert_data_into_database(conn, df):
    
    # Вставка данных из фрейма DataFrame в базу данных
    
    cur = conn.cursor()
    try:
        for index, row in df.iterrows():
            cur.execute("INSERT INTO mytable (column1, column2, column3, column4, column5, column6, column7, column8) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (row['column1'], row['column2'], row['column3'], row['column4'], row['column5'], row['column6'], row['column7'], row['column8']))
        conn.commit()
        return True, {'changed': True}
    except Exception as e:
        error = {'error': f'Ошибка записи в базу данных: {str(e)}', 'changed': False}
        return False, error
    finally:
        cur.close()
        conn.close()

def insert_metrics_into_database(conn, metrics):
    
    # Вставка метрик в базу данных
    
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO metrics (metric_name, metric_value) VALUES (%s, %s)", (metrics['metric_name'], metrics['metric_value']))
        conn.commit()
        return True, {'changed': True}
    except Exception as e:
        error = {'error': f'Ошибка записи метрик в базу данных: {str(e)}', 'changed': False}
        return False, error
    finally:
        cur.close()
        conn.close()

def main(file_path):
    
    # Основная функция
    
    df, error = load_excel_file(file_path)
    if error:
        return False, error
    column_names = ['StudFIO', 'DocID', 'Permission-ID', 'End-Of-Life-Pass', 'Creation', 'Course', 'Group', 'Fak']
    valid, error = validate_column_names(df, column_names)
    if not valid:
        return False, error
    valid, error = validate_empty_rows(df)
    if not valid:
        return False, error
    conn, error = connect_to_database()
    if error:
        return False, error
    valid, result = insert_data_into_database(conn, df)
    if not valid:
        return False, result

    # Отправка метрик в базу данных

    metrics = {'metric_name': 'rows_inserted', 'etric_value': len(df)}
    valid, result = insert_metrics_into_database(conn, metrics)
    if not valid:
        return False, result

    json_data = df.to_json(orient='records')
    return True, {'changed': True, 'data': json_data}

if __name__ == '__main__':
    file_path = '{{ file_path }}'
    success, result = main(file_path)
    response = {'error': '' if success else result, 'changed': result['changed']}
    print(json.dumps(response))


# import pandas as pd
# from psycopg2 import connect
# import json

# def load_excel_file(file_path):
#     """
#     Загрузка файла Excel в Pandas DataFrame
#     """
#     try:
#         df = pd.read_excel(file_path, header=0)
#         return df, None
#     except FileNotFoundError:
#         error = {'error': 'Файл Excel не найден'}
#         return None, error

# def validate_column_names(df, column_names):
#     """
#     Убедитесь, что все необходимые имена столбцов присутствуют в DataFrame
#     """
#     missing_columns = [name for name in column_names if name not in df.columns]
#     if missing_columns:
#         error = {'error': f'Отсутствуют столбцы: {", ".join(missing_columns)}'}
#         return False, error
#     return True, None

# def validate_empty_rows(df):
#     """
#     Убедитесь, что в DataFrame нет пустых строк
#     """
#     empty_rows = [index for index, row in df.iterrows() if not row.any()]
#     if empty_rows:
#         error = {'error': f'Пустые строки: {", ".join(map(str, empty_rows))}'}
#         return False, error
#     return True, None

# def connect_to_database():
#     """
#     Установите соединение с базой данных
#     """
#     try:
#         conn = connect(host='db', database='db', user='admin', password='admin')
#         return conn, None
#     except Exception as e:
#         error = {'error': f'Ошибка подключения к базе данных: {str(e)}'}
#         return None, error

# def insert_data_into_database(conn, df):
#     """
#     Вставка данных из фрейма DataFrame в базу данных
#     """
#     cur = conn.cursor()
#     try:
#         for index, row in df.iterrows():
#             cur.execute("INSERT INTO mytable (column1, column2, column3, column4, column5, column6, column7, column8) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
#                         (row['column1'], row['column2'], row['column3'], row['column4'], row['column5'], row['column6'], row['column7'], row['column8']))
#         conn.commit()
#         return True, None
#     except Exception as e:
#         error = {'error': f'Ошибка записи в базу данных: {str(e)}'}
#         return False, error
#     finally:
#         cur.close()
#         conn.close()

# def main(file_path):
#     """
#     Основная функция
#     """
#     df, error = load_excel_file(file_path)
#     if error:
#         return False, error
#     column_names = ['StudFIO', 'DocID', 'Permission-ID', 'End-Of-Life-Pass', 'Creation', 'Course', 'Group', 'Fak']
#     valid, error = validate_column_names(df, column_names)
#     if not valid:
#         return False, error
#     valid, error = validate_empty_rows(df)
#     if not valid:
#         return False, error
#     conn, error = connect_to_database()
#     if error:
#         return False, error
#     valid, error = insert_data_into_database(conn, df)
#     if not valid:
#         return False, error
#     json_data = df.to_json(orient='records')
#     return True, json_data

# if __name__ == '__main__':
#     file_path = '{{ file_path }}'
#     success, result = main(file_path)
#     if success:
#         print(f"Успешное выполнение: {result}")
#     else:
#         print(f"Ошибка: {result}")