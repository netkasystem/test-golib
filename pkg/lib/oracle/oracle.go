package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/netkasystem/entrustDG/pkg/lib/crypto"

	_ "github.com/sijms/go-ora/v2"
)

type MyError struct {
	message string
}

func (e *MyError) Error() string {
	return e.message
}

type Myconnection struct {
	ip_address  string
	port        string
	user_name   string
	ro_password string
	rw_password string
	database    string
}

type DB struct {
	*sql.DB
	PreparedStatements map[string]*sql.Stmt
	prepstmts          map[string]*sql.Stmt
	driverName         string
	flushInterval      uint
	batchInserts       map[string]*insert
}

type insert struct {
	bindParams []interface{}
	insertCtr  uint
	queryPart1 string
	queryPart2 string
	queryPart3 string
	values     string
}

// Global DB pool
var dbpool *pgxpool.Pool

// ConnectDB initializes a connection pool to Oracle
func ConnectDB(uri string) (*pgxpool.Pool, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	dbpool, err = pgxpool.New(ctx, uri)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
		return nil, err
	}
	fmt.Println("Connected to Oracle successfully!")
	return dbpool, nil
}

// DisconnectDB closes the database connection
func DisconnectDB() {
	if dbpool != nil {
		dbpool.Close()
		fmt.Println("Disconnected from Oracle!")
	}
}

// InsertBatch inserts multiple records using batch processing
func InsertBatch(dbpool *pgxpool.Pool, table string, columns []string, values [][]interface{}) error {
	if dbpool == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	ctx := context.Background()
	batch := &pgx.Batch{} // ✅ Use pgx.Batch, not pgxpool.Batch

	// Generate queries dynamically
	columnsStr := "(" + formatColumns(columns) + ")"
	placeholder := generatePlaceholders(len(columns))

	for _, row := range values {
		//INSERT INTO app_data_asset ([id name description parent]) VALUES ($1,$2,$3,$4)
		query := fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, columnsStr, placeholder)

		batch.Queue(query, row...)

	}

	// Execute batch
	conn, err := dbpool.Acquire(ctx) // Get a connection from the pool
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %v", err)
	}
	defer conn.Release()

	results := conn.SendBatch(ctx, batch)
	defer results.Close()

	// Process results
	for i := 0; i < len(values); i++ {
		_, err := results.Exec()
		if err != nil {
			return fmt.Errorf("❌batch insert "+table+" table:%v failed: %v", err)
		}
	}

	fmt.Println("✅ Batch insert " + table + " completed successfully!")
	return nil
}

// UpdateField updates specified fields in a given table based on the provided conditions.
func UpdateField(dbpool *pgxpool.Pool, table string, fields map[string]interface{}, conditions string, conditionArgs ...interface{}) error {
	if dbpool == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	if len(fields) == 0 {
		return fmt.Errorf("no fields provided for update")
	}

	ctx := context.Background()

	// Dynamically build SET clause with placeholders
	setClauses := []string{}
	args := []interface{}{}
	i := 1
	for column, value := range fields {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", column, i))
		args = append(args, value)
		i++
	}

	setClause := strings.Join(setClauses, ", ")

	// Append condition arguments if any
	args = append(args, conditionArgs...)

	// Build final UPDATE query
	query := fmt.Sprintf("UPDATE %s SET %s", table, setClause)
	if conditions != "" {
		query += fmt.Sprintf(" WHERE %s", conditions)
	}

	// Execute the update query
	cmdTag, err := dbpool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update %s table: %v", table, err)
	}

	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("no rows were updated in %s table", table)
	}

	fmt.Printf("✅ Successfully updated %d row(s) in %s table.\n", cmdTag.RowsAffected(), table)
	return nil
}

// Helper function to format column names
func formatColumns(columns []string) string {
	//return fmt.Sprintf("%s", columns)
	return fmt.Sprintf("%s", strings.Join(columns, ", "))
}

// Helper function to generate placeholders like ($1, $2, $3)
func generatePlaceholders(n int) string {
	placeholders := ""
	for i := 1; i <= n; i++ {
		placeholders += fmt.Sprintf("$%d,", i)
	}
	return "(" + placeholders[:len(placeholders)-1] + ")" // Remove last comma
}

func DeleteWhere(dbpool *pgxpool.Pool, table string, conditions string) error {
	if dbpool == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	ctx := context.Background()

	// Build WHERE clause dynamically
	whereClause := conditions
	query := ""
	if len(conditions) > 0 {

		query = fmt.Sprintf("DELETE FROM %s WHERE %s", table, whereClause)
	} else {

		query = fmt.Sprintf("DELETE FROM %s", table)
	}

	// Execute query
	_, err := dbpool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to delete table %v rows: %v", table, err)
	}

	fmt.Println("Rows deleted successfully!")
	return nil
}

func Open(driverName, dataSourceName string, flushInterval uint) (*DB, error) {
	var (
		err error
		dbh *sql.DB
	)

	if dbh, err = sql.Open(driverName, dataSourceName); err != nil {
		return nil, err
	}

	return &DB{
		DB:                 dbh,
		PreparedStatements: make(map[string]*sql.Stmt),
		prepstmts:          make(map[string]*sql.Stmt),
		driverName:         driverName,
		flushInterval:      flushInterval,
		batchInserts:       make(map[string]*insert),
	}, err
}

func OpenOracle(connection_string string, flushInterval uint) (*DB, error) {
	var connection Myconnection
	dbFields := strings.Split(connection_string, ",")

	if len(dbFields) < 4 {
		return nil, fmt.Errorf("invalid connection string format")
	}

	connection.ip_address = strings.TrimPrefix(dbFields[0], "dbconnect=")
	connection.port = dbFields[1]
	connection.database = dbFields[2]
	connection.user_name = dbFields[3]
	connection.ro_password = dbFields[4]
	// connection.rw_password = dbFields[5]

	var err error
	if connection_string == "" {
		err = &MyError{"Not Found Connection "}
		return nil, err
	}
	if crypto.IsBase64Encoding(connection.ro_password) {
		cryptor := crypto.NewDeCrypter()
		var temp string
		temp, err = cryptor.Decrypt(connection.ro_password)
		if err != nil {
		} else {
			connection.ro_password = temp
		}
	}

	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s", connection.user_name, connection.ro_password, connection.ip_address, connection.port)
	if len(connection.database) > 0 {
		dsn = fmt.Sprintf("oracle://%s:%s@%s:%s?database=%s", connection.user_name, connection.ro_password, connection.ip_address, connection.port, connection.database)
	}

	db, err := Open("oracle", dsn, flushInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	// Verify connection
	// if err := db.Ping(); err != nil {
	// 	return nil, fmt.Errorf("cannot reach database: %v", err)
	// }

	return db, err
}

func DBList(db *sql.DB) ([]string, error) {
	// rows, err := db.Query("SHOW DATABASES")
	rows, err := db.Query("SELECT name FROM sys.databases WHERE name NOT IN ('master','model','msdb','tempdb')")
	if err != nil {
		return nil, fmt.Errorf("failed to query databases: %v", err)
	}
	defer rows.Close()

	// Slice to store database names
	var databases []string

	// Iterate over the rows and scan the database names
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			return nil, fmt.Errorf("failed to scan database name: %v", err)
		}
		databases = append(databases, database)
	}

	// Check for errors after iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error after iterating rows: %v", err)
	}

	return databases, nil
}
