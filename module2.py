import pandas as pd
from psycopg2 import connect
import json
import influxdb

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

def insert_metrics_into_influx(metrics):

    # Вставка метрик в InfluxDB
    
    client = influxdb.InfluxDBClient(host='influxdb', port=8086)
    db_name = 'ydb'
    client.switch_database(db_name)
    
    json_body = [
        {
            "measurement": "metrics",
            "tags": {
                "metric_name": metrics['metric_name']
            },
            "fields": {
                "value": metrics['metric_value']
            }
        }
    ]
    
    try:
        client.write_points(json_body)
        return True, {'changed': True}
    except Exception as e:
        error = {'error': f'Ошибка записи метрик в InfluxDB: {str(e)}', 'changed': False}
        return False, error

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

    # Отправка метрик в InfluxDB

    metrics = {'metric_name': 'rows_inserted', 'etric_value': len(df)}
    valid, result = insert_metrics_into_influx(metrics)
    if not valid:
        return False, result

    json_data = df.to_json(orient='records')
    return True, {'changed': True, 'data': json_data}

if __name__ == '__main__':
    file_path = '{{ file_path }}'
    success, result = main(file_path)
    response = {'error': '' if success else result, 'changed': result['changed']}
    print(json.dumps(response))