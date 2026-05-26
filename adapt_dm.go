package mx

import (
	"database/sql"
	"strings"
	"time"
)

func (r *SQLRows) dmRowsMap() RowsMap {
	rs := make(RowsMap, 0)
	if r.err != nil {
		return rs
	}
	if r.rows == nil {
		return rs
	}
	defer r.rows.Close()
	cols, err := r.rows.Columns()
	if err != nil {
		return rs
	}
	ts, err := r.rows.ColumnTypes()
	if err != nil {
		return rs
	}
	// for _, t := range ts {
	// 	fmt.Println(t.Name(), t.DatabaseTypeName(), t.ScanType())
	// }
	for i := range cols {
		cols[i] = strings.ToLower(cols[i])
	}

	for r.rows.Next() {
		rowMap := make(map[string]string)
		containers := make([]any, 0, len(cols))
		for i := 0; i < cap(containers); i++ {
			switch ts[i].DatabaseTypeName() {
			case "TIMESTAMP", "DATE":
				containers = append(containers, &sql.NullTime{})
			default:
				containers = append(containers, &[]byte{})
			}
		}
		if err := r.rows.Scan(containers...); err != nil {
			return RowsMap{}
		}
		for i := range cols {
			switch v := containers[i].(type) {
			case *[]byte:
				rowMap[cols[i]] = string(*containers[i].(*[]byte))
			case *sql.NullTime:
				if v.Valid {
					switch ts[i].DatabaseTypeName() {
					case "DATE":
						rowMap[cols[i]] = v.Time.Format(time.DateOnly)
					case "TIMESTAMP":
						rowMap[cols[i]] = v.Time.Format(time.DateTime)
					default:
						rowMap[cols[i]] = v.Time.Format(time.DateTime)
					}
				} else {
					rowMap[cols[i]] = ""
				}
			}
		}
		rs = append(rs, rowMap)
	}
	if err := r.rows.Err(); err != nil {
		return RowsMap{}
	}
	return rs
}
