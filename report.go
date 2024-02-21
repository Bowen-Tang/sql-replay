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
        SQL    string
        Columns []string
        Rows   [][]interface{}
        Error  error
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
                "1: rt_sample<500us": `SELECT
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
                "2: rt_sample 500us~1ms": `SELECT
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
                "3: rt_sample 1ms~10ms": `SELECT
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
                "4: rt_sample 10ms~100ms": `SELECT
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
                "5: rt_sample 100ms~1s": `SELECT
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
                "6: rt_sample 1s~10s": `SELECT
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
                "7: rt_sample>10s": `SELECT
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
        "8: error info": `select sql_digest,error_info,count(*) exec_cnts,min(sql_text) as sample_sql_text from replay_info where error_info <>'' and file_name like concat(?,'%') group by sql_digest,error_info order by sql_digest,error_info,count(*) desc`,
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
                width: 250px; /* 设置导航栏的宽度 */
                background-color: #f0f0f0; /* 设置导航栏的背景颜色 */
                padding: 20px;
                box-sizing: border-box;
                overflow-y: auto; /* 添加垂直滚动条 */
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
                margin-left: 270px; /* 留出导航栏的空间 */
            }
            table {
                border-collapse: collapse;
                width: 100%;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
        </style>
    </head>
    <body>
        <nav>
            <ul>
                {{range $key, $query := .}}
                <li><a href="#{{ $key }}">{{ $key }}</a></li>
                {{end}}
            </ul>
        </nav>
        <main>
            {{range $key, $query := .}}
            <h1 id="{{ $key }}">{{ $key }}</h1>
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
                    {{range .}}
                    <td>{{.}}</td>
                    {{end}}
                </tr>
                {{end}}
            </table>
            {{end}}
            {{end}}
        </main>
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