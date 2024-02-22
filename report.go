package main

import (
    "database/sql"
    "fmt"
    "html/template"
    "log"
    "net/http"

    _ "github.com/go-sql-driver/mysql"
)

type QueryResult struct {
    SQL     string
    Columns []string
    Rows    [][]interface{}
    Error   error
}

func Report(dbConnStr, replayOut, Port string) {
    if dbConnStr == "" || replayOut == "" {
        fmt.Println("Usage: ./sql-replay -mode report -db <mysql_connection_string> -replay-name <replay name> -port ':8081'")
        return
    }
    // 连接数据库
    db, err := sql.Open("mysql", dbConnStr)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 定义 SQL 查询
    queries := map[string]string{
        "1. RT <500us": `SELECT
            sql_digest,sql_type,
            COUNT(*) AS exec_cnts,
            round(AVG(execution_time / 1000),2) AS current_ms,
            round(AVG(query_time / 1000),2) AS before_ms,
            concat(ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2)*100,'%') AS reduce_pct,
            MIN(sql_text) AS sample_sql_text
        FROM
            replay_info
        WHERE
            file_name like concat(?,'%') and error_info=''
        GROUP BY
            sql_digest
        HAVING
            AVG(query_time) <= 500
        ORDER BY
            avg(execution_time)/avg(query_time) desc`,
        "2. RT 500us~1ms": `SELECT
            sql_digest,sql_type,
            COUNT(*) AS exec_cnts,
            round(AVG(execution_time / 1000),2) AS current_ms,
            round(AVG(query_time / 1000),2) AS before_ms,
            concat(ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2)*100,'%') AS reduce_pct,
            MIN(sql_text) AS sample_sql_text
        FROM
            replay_info
        WHERE
            file_name like concat(?,'%') and error_info=''
        GROUP BY
            sql_digest
        HAVING
            AVG(query_time) > 500 AND AVG(query_time) <= 1000
        ORDER BY
            avg(execution_time)/avg(query_time) desc`,
        "3. RT 1ms~10ms": `SELECT
            sql_digest,sql_type,
            COUNT(*) AS exec_cnts,
            round(AVG(execution_time / 1000),2) AS current_ms,
            round(AVG(query_time / 1000),2) AS before_ms,
            concat(ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2)*100,'%') AS reduce_pct,
            MIN(sql_text) AS sample_sql_text
        FROM
            replay_info
        WHERE
            file_name like concat(?,'%') and error_info=''
        GROUP BY
            sql_digest
        HAVING
            AVG(query_time) > 1000 AND AVG(query_time) <= 10000
        ORDER BY
            avg(execution_time)/avg(query_time) desc`,
        "4. RT 10ms~100ms": `SELECT
            sql_digest,sql_type,
            COUNT(*) AS exec_cnts,
            round(AVG(execution_time / 1000),2) AS current_ms,
            round(AVG(query_time / 1000),2) AS before_ms,
            concat(ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2)*100,'%') AS reduce_pct,
            MIN(sql_text) AS sample_sql_text
        FROM
            replay_info
        WHERE
            file_name like concat(?,'%') and error_info=''
        GROUP BY
            sql_digest
        HAVING
            AVG(query_time) > 10000 AND AVG(query_time) <= 100000
        ORDER BY
            avg(execution_time)/avg(query_time) desc`,
        "5. RT 100ms~1s": `SELECT
            sql_digest,sql_type,
            COUNT(*) AS exec_cnts,
            round(AVG(execution_time / 1000),2) AS current_ms,
            round(AVG(query_time / 1000),2) AS before_ms,
            concat(ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2)*100,'%') AS reduce_pct,
            MIN(sql_text) AS sample_sql_text
        FROM
            replay_info
        WHERE
            file_name like concat(?,'%') and error_info=''
        GROUP BY
            sql_digest
        HAVING
            AVG(query_time) > 100000 AND AVG(query_time) <= 1000000
        ORDER BY
            avg(execution_time)/avg(query_time) desc`,
        "6. RT 1s~10s": `SELECT
            sql_digest,sql_type,
            COUNT(*) AS exec_cnts,
            round(AVG(execution_time / 1000),2) AS current_ms,
            round(AVG(query_time / 1000),2) AS before_ms,
            concat(ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2)*100,'%') AS reduce_pct,
            MIN(sql_text) AS sample_sql_text
        FROM
            replay_info
        WHERE
            file_name like concat(?,'%') and error_info=''
        GROUP BY
            sql_digest
        HAVING
            AVG(query_time) > 1000000 AND AVG(query_time) <= 10000000
        ORDER BY
            avg(execution_time)/avg(query_time) desc`,
        "7. RT >10s": `SELECT
            sql_digest,sql_type,
            COUNT(*) AS exec_cnts,
            round(AVG(execution_time / 1000),2) AS current_ms,
            round(AVG(query_time / 1000),2) AS before_ms,
            concat(ROUND((AVG(execution_time / 1000) - AVG(query_time / 1000)) / AVG(query_time / 1000) ,2)*100,'%') AS reduce_pct,
            MIN(sql_text) AS sample_sql_text
        FROM
           replay_info
        WHERE
            file_name like concat(?,'%') and error_info=''
        GROUP BY
            sql_digest
        HAVING
            AVG(query_time) > 10000000
        ORDER BY
            avg(execution_time)/avg(query_time) desc`,
        "8. ERROR INFO": `select sql_digest,count(*) exec_cnts,substr(error_info,1,64) as error_info,min(sql_text) as sample_sql_text from replay_info where error_info <>'' and file_name like concat(?,'%') group by sql_digest,error_info order by sql_digest,error_info,count(*) desc`,
    }

    tmpl := `
<!DOCTYPE html>
<html>
<head>
    <title>replay report</title>
    <style>
        body {
            margin: 0;
            padding: 0;
            display: flex;
        }
        nav {
            position: fixed; /* 将导航栏固定在页面左侧 */
            left: 0; /* 将导航栏固定在页面左侧 */
            top: 0;
            height: 100%; /* 设置导航栏高度为整个页面的高度 */
            width: 240px; /* 设置导航栏的宽度 */
            background-color: #F5F5F5; /* 设置导航栏的背景颜色 */
            padding: 20px;
            padding-top: 36px;
            box-sizing: border-box;
            overflow-y: auto; /* 添加垂直滚动条 */
        }
        nav a {
            text-decoration: none; /* 去掉链接的下划线 */
            font-weight: bold; /* 设置字体加粗 */
            color: #1e88e5; /* 设置为深蓝色 */
        }
        nav ul {
            list-style: none;
            padding: 0;
            margin: 0;
        }
        nav ul li {
            margin-bottom: 10px;
        }
        main {
            flex: 1; /* 设置主内容区域自动填充剩余空间 */
            padding: 20px;
            margin-left: 240px; /* 留出导航栏的空间 */
        }
        .blue-bar {
            background-color: rgba(173, 216, 230, 0.05);
            color: #333; /* 设置文字颜色为深灰色 */
            font-size: 20px; /* 设置文字大小为 32 像素 */
            font-weight: bold; /* 设置文字加粗 */
            padding: 10px; /* 设置内边距 */
            margin-top: 10px; /* 设置顶部边距 */
            margin-bottom: 10px; /* 设置底部边距 */
            width: 100%; /* 设置宽度与页面一致 */
            box-sizing: border-box; /* 设置盒子模型为边框盒模型 */
            text-align: center; /* 文字居中 */
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); /* 添加阴影 */
        }
        .nav-heading {
            font-size: 20px; /* 调大字体 */
            font-weight: bold; /* 加粗 */
            color: navy; /* 设置为深蓝色 */
            margin-bottom: 20px; /* 设置下边距为 10px */
        }
        table {
            border-collapse: collapse;
            width: 100%;
            table-layout: fixed;
            margin-bottom: 30px; /* 为表格底部添加下边距 */
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
        }
        th {
            background-color: #f2f2f2;
        }
        /* 添加预览框样式 */
        #preview {
            position: fixed;
            background-color: white;
            border: 1px solid #ccc;
            padding: 10px;
            display: none;
            z-index: 9999;
            max-width: 1000px;
            width: 500px; /* 固定预览框的宽度为 500px */
            min-width: 500px; /* 设置最小宽度 */
            font-size: 15px; /* 设置预览框中文本的字体大小为 15px */
        }
    </style>
</head>
<body>
    <nav>
        <ul>
            <li class="nav-heading">SQL Replay Report</li> <!-- 新增的一行 -->
            {{range $key, $query := .}}
            <li><a href="#{{ $key }}">{{ $key }}</a></li>
            {{end}}
        </ul>
    </nav>
    <main>
        {{range $key, $query := .}}
        {{ if eq $key "1. RT <500us" }}
        <div class="blue-bar" id="{{ $key }}">{{ $key }}</div>
        {{ else if eq $key "2. RT 500us~1ms" }}
        <div class="blue-bar" id="{{ $key }}">{{ $key }}</div>
        {{ else if eq $key "3. RT 1ms~10ms" }}
        <div class="blue-bar" id="{{ $key }}">{{ $key }}</div>
        {{ else if eq $key "4. RT 10ms~100ms" }}
        <div class="blue-bar" id="{{ $key }}">{{ $key }}</div>
        {{ else if eq $key "5. RT 100ms~1s" }}
        <div class="blue-bar" id="{{ $key }}">{{ $key }}</div>
        {{ else if eq $key "6. RT 1s~10s" }}
        <div class="blue-bar" id="{{ $key }}">{{ $key }}</div>
        {{ else if eq $key "7. RT >10s" }}
        <div class="blue-bar" id="{{ $key }}">{{ $key }}</div>
        {{ else if eq $key "8. ERROR INFO" }}
        <div class="blue-bar" id="{{ $key }}">{{ $key }}</div>
        {{ else }}
        <h1 id="{{ $key }}">{{ $key }}</h1>
        {{ end }}
        {{with $query.Error}}
        <p>Error: {{ . }}</p>
        {{else}}
        <table>
            <tr>
                {{range $query.Columns}}
                <th>{{.}}</th>
                {{end}}
            </tr>
                {{range $query.Rows}}
                <tr>
                    {{range $index, $value := .}}
                        {{if eq (index $query.Columns $index) "sample_sql_text"}}
                            <td class="previewable">{{$value}}</td>
                        {{else}}
                            <td>{{$value}}</td>
                        {{end}}
                    {{end}}
                </tr>
                {{end}}
        </table>
        {{end}}
        {{end}}
    </main>

    <!-- 添加预览框 -->
    <div id="preview"></div>

    <script>
        // 获取所有预览单元格
        var previewableCells = document.querySelectorAll('.previewable');

        // 监听鼠标移动事件
        document.addEventListener('mousemove', function(event) {
            // 遍历所有预览单元格
            previewableCells.forEach(function(cell) {
                // 判断鼠标是否在预览单元格上方
                if (isMouseOverCell(cell, event)) {
                    // 显示预览框，并设置内容和位置
                    showPreview(cell.textContent, event.clientX, event.clientY);
                }
            });
        });

        // 监听单元格的鼠标离开事件
        previewableCells.forEach(function(cell) {
            cell.addEventListener('mouseleave', function(event) {
                var preview = document.getElementById('preview');
                preview.style.display = 'none';
            });
        });

        // 判断鼠标是否在预览单元格上方的函数
        function isMouseOverCell(cell, event) {
            var rect = cell.getBoundingClientRect();
            return event.clientX >= rect.left && event.clientX <= rect.right &&
                event.clientY >= rect.top && event.clientY <= rect.bottom;
        }

        // 显示预览框的函数
        function showPreview(content, x, y) {
            var preview = document.getElementById('preview');
            preview.innerHTML = content;
            preview.style.display = 'block';
            // 计算悬浮框的位置，使其固定在页面右侧边框处，向左展开 500px
            var rightEdge = document.body.clientWidth - x;
            if (rightEdge > 500) {
                preview.style.left = (x + 10) + 'px';
            } else {
                preview.style.left = (x - 510) + 'px';
            }
            preview.style.top = (y + 10) + 'px';
        }
    </script>

</body>
</html>
`

    t, err := template.New("webpage").Parse(tmpl)
    if err != nil {
        log.Fatal(err)
    }

    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        results := make(map[string]QueryResult)
        for name, query := range queries {
            rows, err := db.Query(query, replayOut)
            if err != nil {
                results[name] = QueryResult{SQL: name, Error: err}
                continue
            }
            defer rows.Close()

            columns, err := rows.Columns()
            if err != nil {
                results[name] = QueryResult{SQL: name, Error: err}
                continue
            }

            var rowsData [][]interface{}
            for rows.Next() {
                values := make([]interface{}, len(columns))
                valuePtrs := make([]interface{}, len(columns))
                for i := range values {
                    valuePtrs[i] = &values[i]
                }
                if err := rows.Scan(valuePtrs...); err != nil {
                    results[name] = QueryResult{SQL: name, Error: err}
                    continue
                }
                rowData := make([]interface{}, len(columns))
                for i, v := range values {
                    b, ok := v.([]byte)
                    if ok {
                        rowData[i] = string(b)
                    } else {
                        rowData[i] = v
                    }
                }
                rowsData = append(rowsData, rowData)
            }

            if err := rows.Err(); err != nil {
                results[name] = QueryResult{SQL: name, Error: err}
            }

            results[name] = QueryResult{SQL: name, Columns: columns, Rows: rowsData}
        }

        if err := t.Execute(w, results); err != nil {
            log.Fatal(err)
        }
    })

    fmt.Println("Server is running on port " + Port)
    if err := http.ListenAndServe(Port, nil); err != nil {
        log.Fatal(err)
    }
}