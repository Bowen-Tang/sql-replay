import argparse
import pandas as pd
import mysql.connector

parser = argparse.ArgumentParser(description='Generate HTML report from SQL query.')
parser.add_argument('--user', help='Database user', required=True)
parser.add_argument('--password', help='Database password', default='')  # 设置默认值为空字符串
parser.add_argument('--host', help='Database host', required=True)
parser.add_argument('--database', help='Database name', required=True)
parser.add_argument('--port', type=int, help='Database port', required=True)
parser.add_argument('--outfile_prefix', help='outfile name', required=True)
parser.add_argument('--tablename', help='query table name', required=True)


# 解析命令行参数
args = parser.parse_args()

config = {
    'user': args.user,
    'password': args.password,
    'host': args.host,
    'database': args.database,
    'port': args.port,
    'raise_on_warnings': True
}

outfile_prefix = args.outfile_prefix
tablename = args.tablename


# SQL 查询列表
sql_queries = [
'''--     1: rt_sample<500u
SELECT
    sql_digest,sql_type,
    COUNT(*) AS exec_cnts,
    round(AVG(execution_time / 1000),2) AS current_ms,
    round(AVG(query_time / 1000),2) AS before_ms,
    ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2) AS reduce_pct,
    MIN(sql_text) AS sample_sql_text
FROM
    go4
WHERE
    file_name like concat(%s,'%')
GROUP BY
    sql_digest
HAVING
    AVG(query_time) <= 500
ORDER BY
    avg(execution_time)/avg(query_time) desc''',
'''--     2: rt_sample 500us~1ms
SELECT
    sql_digest,sql_type,
    COUNT(*) AS exec_cnts,
    round(AVG(execution_time / 1000),2) AS current_ms,
    round(AVG(query_time / 1000),2) AS before_ms,
    ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2) AS reduce_pct,
    MIN(sql_text) AS sample_sql_text
FROM
    go4
WHERE
    file_name like concat(%s,'%')
GROUP BY
    sql_digest
HAVING
    AVG(query_time) > 500 AND AVG(query_time) <= 1000
ORDER BY
    avg(execution_time)/avg(query_time) desc''',
'''--     3: rt_sample 1ms~10ms
SELECT
    sql_digest,sql_type,
    COUNT(*) AS exec_cnts,
    round(AVG(execution_time / 1000),2) AS current_ms,
    round(AVG(query_time / 1000),2) AS before_ms,
    ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2) AS reduce_pct,
    MIN(sql_text) AS sample_sql_text
FROM
    go4
WHERE
    file_name like concat(%s,'%')
GROUP BY
    sql_digest
HAVING
    AVG(query_time) > 1000 AND AVG(query_time) <= 10000
ORDER BY
    avg(execution_time)/avg(query_time) desc''',
'''--     4: rt_sample 10ms~100ms
SELECT
    sql_digest,sql_type,
    COUNT(*) AS exec_cnts,
    round(AVG(execution_time / 1000),2) AS current_ms,
    round(AVG(query_time / 1000),2) AS before_ms,
    ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2) AS reduce_pct,
    MIN(sql_text) AS sample_sql_text
FROM
    go4
WHERE
    file_name like concat(%s,'%')
GROUP BY
    sql_digest
HAVING
    AVG(query_time) > 10000 AND AVG(query_time) <= 100000
ORDER BY
    avg(execution_time)/avg(query_time) desc''',
'''--     5: rt_sample 100ms~1s
SELECT
    sql_digest,sql_type,
    COUNT(*) AS exec_cnts,
    round(AVG(execution_time / 1000),2) AS current_ms,
    round(AVG(query_time / 1000),2) AS before_ms,
    ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2) AS reduce_pct,
    MIN(sql_text) AS sample_sql_text
FROM
    go4
WHERE
    file_name like concat(%s,'%')
GROUP BY
    sql_digest
HAVING
    AVG(query_time) > 100000 AND AVG(query_time) <= 1000000
ORDER BY
    avg(execution_time)/avg(query_time) desc''',
'''--     6: rt_sample 1s~10s
SELECT
    sql_digest,sql_type,
    COUNT(*) AS exec_cnts,
    round(AVG(execution_time / 1000),2) AS current_ms,
    round(AVG(query_time / 1000),2) AS before_ms,
    ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2) AS reduce_pct,
    MIN(sql_text) AS sample_sql_text
FROM
    go4
WHERE
    file_name like concat(%s,'%')
GROUP BY
    sql_digest
HAVING
    AVG(query_time) > 1000000 AND AVG(query_time) <= 10000000
ORDER BY
    avg(execution_time)/avg(query_time) desc''',
'''--     7: rt_sample>10s
SELECT
    sql_digest,sql_type,
    COUNT(*) AS exec_cnts,
    round(AVG(execution_time / 1000),2) AS current_ms,
    round(AVG(query_time / 1000),2) AS before_ms,
    ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2) AS reduce_pct,
    MIN(sql_text) AS sample_sql_text
FROM
   go4
WHERE
    file_name like concat(%s,'%')
GROUP BY
    sql_digest
HAVING
    AVG(query_time) > 10000000
ORDER BY
    avg(execution_time)/avg(query_time) desc'''
]


# 连接数据库并执行查询

def execute_query(sql,outfile_prefix):
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(sql,(outfile_prefix,))
    df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
    cursor.close()
    conn.close()

    # Process reduce_pct column
    if 'reduce_pct' in df.columns and df['reduce_pct'].dtype != object:
        df['reduce_pct'] = df['reduce_pct'].astype(str) + '%'

    return df, len(df)

def style_reduce_pct(value):
    try:
        val = float(value)
        if val >= 1:
            return f'<span class="red">{round(val*100,0)}%</span>'
        elif 0.2 <= val < 1:
            return f'<span class="blue">{round(val*100,0)}%</span>'
        elif 0 < val < 0.2:
            return f'<span class="black">{round(val*100,0)}%</span>'
        elif -1 < val < 0:
            return f'<span class="green">{round(val*100,0)}%</span>'
        else:  # val <= -1
            return f'<span class="boldGreen">{round(val*100,0)}%</span>'
    except ValueError:
        return value

def generate_html():
    html_content = '<html><head>'
    html_content += f'<title>{outfile_prefix}: compare results</title>'
    html_content += '<style>'
    # 在这里添加 CSS 样式
    html_content += 'table {border-collapse: collapse; width: 100%;} th, td {border: 1px solid black; padding: 8px; text-align: left;}'
    html_content += 'th {background-color: #4CAF50; color: white;}'
    html_content += 'tr:nth-child(even){background-color: #f2f2f2;}'
    html_content += 'tr:hover {background-color: #ddd;}'
    html_content += 'div.nav {position: fixed; left: 0; top: 0; width: 250px; height: 100%; overflow: auto; background-color: #333; padding: 20px; color: white;}'
    html_content += 'div.nav a {color: white; text-decoration: none; display: block; margin-bottom: 10px;}'
    html_content += 'div.nav a:hover {background-color: #ddd; color: black;}'
    html_content += '.red {color: red; font-weight: bold;}'
    html_content += '.blue {color: blue;}'
    html_content += '.black {color: black;}'
    html_content += '.green {color: green;}'
    html_content += '.boldGreen {color: green; font-weight: bold;}'
    html_content += 'td.sample_sql_text { width: 500px; word-wrap: break-word; overflow-wrap: break-word;}'
    html_content += '</style></head><body>'

    # 导航栏
    html_content += '<div class="nav">'
    # 添加链接或锚点到 outfile_prefix 名称
    html_content += f'<p><a href="#outfile_prefix">{outfile_prefix}: compare results</a></p>'
    record_counts = []
    query_results = []

    # 定义每个表格的标题
    table_titles = [
        "before: < 500us",
        "before: 500us ~ 1ms",
        "before: 1ms ~ 10ms",
        "before: 10ms ~ 100ms",
        "before: 100ms ~ 1s",
        "before: 1s ~ 10s",
        "before: >10s"
    ]

    for i, sql in enumerate(sql_queries):
        df, count = execute_query(sql, outfile_prefix)
        record_counts.append(count)
        query_results.append(df)

    for i, df in enumerate(query_results):
        html_content += f'<p><a href="#table{i+1}">{table_titles[i]} - {record_counts[i]} rows</a></p>'

    html_content += '</div>'

    # 主内容
    html_content += '<div style="margin-left: 270px; padding: 20px;">'
    # 添加 outfile_prefix 锚点
    html_content += f'<h2 id="outfile_prefix" style="color: #333; font-weight: bold;">{outfile_prefix}: compare results</h2>'

    for i, df in enumerate(query_results):
        df['reduce_pct'] = df['reduce_pct'].apply(style_reduce_pct)  # 应用样式函数
        html_content += f'<h2 id="table{i+1}">{table_titles[i]} - {record_counts[i]} rows</h2>'
        html_content += df.to_html(index=False, escape=False)  # 使用 escape=False 来渲染 HTML
    html_content += '</div>'

    # 结束 HTML 内容
    html_content += '</body></html>'
    return html_content

report_file = outfile_prefix + '.html'
def save_html_report():
    html_report = generate_html()
    with open(report_file, 'w') as file:
        file.write(html_report)

save_html_report()
