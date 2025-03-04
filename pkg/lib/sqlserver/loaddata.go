package postgres

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type loadDataInsert struct {
	table        string
	dataFilePath string
	insertCtr    uint
	columnCount  int
	bindParams   []interface{}
}

func (d *DB) LoadDataInsert(table string, params ...interface{}) (err error) {
	if _, ok := d.loadDataInserts[table]; !ok {
		d.loadDataInserts[table] = newLoadDataInsert()
	}

	if d.loadDataInserts[table].table == "" {
		d.loadDataInserts[table].table = table
		d.loadDataInserts[table].columnCount = len(params)

		current_dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			return err
		}

		data_dir, err := os.Stat(filepath.Join(current_dir, "data"))
		if err == nil {
			if !data_dir.IsDir() {
				return errors.New(fmt.Sprintf("%s exists but it's not a directory\n", filepath.Join(current_dir, "data")))
			}
		} else {
			if os.IsNotExist(err) {
				err = os.MkdirAll(filepath.Join(current_dir, "data"), 0755)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}

		d.loadDataInserts[table].dataFilePath = filepath.Join(current_dir, "data", fmt.Sprintf("%s.dat", table))
	}

	d.loadDataInserts[table].insertCtr++

	d.loadDataInserts[table].bindParams = append(d.loadDataInserts[table].bindParams, params...)

	if d.loadDataInserts[table].insertCtr >= d.flushInterval {
		err = d.flushLoadDataToDisk(d.loadDataInserts[table])
	}
	return err
}

func (d *DB) flushLoadDataToDisk(in *loadDataInsert) (err error) {
	var f *os.File

	if _, err := os.Stat(in.dataFilePath); err != nil {
		f, err = os.Create(in.dataFilePath)
		if err != nil {
			return err
		}
	} else {
		f, err = os.OpenFile(in.dataFilePath, os.O_APPEND, 0666)
		if err != nil {
			return err
		}
	}
	defer f.Close()

	var col = 0

	w := bufio.NewWriter(f)
	//n4, err := w.WriteString("buffered\n")
	//fmt.Printf("wrote %d bytes\n", n4)
	for _, insert_data := range in.bindParams {
		temp := fmt.Sprint(insert_data)

		w.WriteString(temp)

		w.WriteString("\t")
		col++
		if col == in.columnCount {
			w.WriteString("\n")
			col = 0
		}
	}

	w.Flush()
	//f.Sync()

	// Reset vars
	in.bindParams = make([]interface{}, 0)
	in.insertCtr = 0

	return err
}

func (d *DB) flushLoadData(in *loadDataInsert) (err error) {
	if in.insertCtr > 0 {
		if err := d.flushLoadDataToDisk(in); err != nil {
			return err
		}
	}

	if _, err := os.Stat(in.dataFilePath); err != nil {
		return nil
	}

	query := fmt.Sprintf("LOAD DATA CONCURRENT LOCAL INFILE '%s' INTO TABLE %s", strings.Replace(in.dataFilePath, "\\", "\\\\", 20), in.table)

	// pq.RegisterLocalFile(in.dataFilePath)

	// Executate batch insert
	if _, err = d.DB.Exec(query); err != nil {
		return err
	} //if

	// remove file
	if err := os.Remove(in.dataFilePath); err != nil {
		return err
	}

	return err
}

func newLoadDataInsert() *loadDataInsert {
	return &loadDataInsert{
		bindParams: make([]interface{}, 0),
	}
}
