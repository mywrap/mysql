package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ConnectGORM initializes a new MySQL client (sent a Ping)
func ConnectViaGORM(config Config) (*gorm.DB, error) {
	if config.Host == "" || config.Port == "" {
		return nil, errors.New("empty config")
	}
	db, err := gorm.Open(
		mysql.Open(config.ToDataSourceURL()),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, err
	}
	sqlDb, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("unexpected db_DB(): %v", err)
	}
	if sqlDb != nil {
		configConnectionsPool(sqlDb)
	}
	return db, nil
}

// Connect initializes a new MySQL client (sent a Ping)
func Connect(config Config) (*sql.DB, error) {
	if config.Host == "" || config.Port == "" {
		return nil, errors.New("empty config")
	}
	db, err := sql.Open("mysql", config.ToDataSourceURL())
	if err != nil {
		// a data source name parse error or another initialization error
		return nil, err
	}
	configConnectionsPool(db)
	if err := db.Ping(); err != nil { // connection error returns here
		return nil, err
	}
	return db, nil
}

// a recommended connections pool setting
func configConnectionsPool(db *sql.DB) {
	nMaxConns := 40 // server with 1GB memory
	db.SetMaxOpenConns(nMaxConns)
	db.SetMaxIdleConns(nMaxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)
}

// LoadEnvConfig loads config from environment variables:
// MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
func LoadEnvConfig() Config {
	return Config{
		Host:     os.Getenv("MYSQL_HOST"),
		Port:     os.Getenv("MYSQL_PORT"),
		Username: os.Getenv("MYSQL_USER"),
		Password: os.Getenv("MYSQL_PASSWORD"),
		Database: os.Getenv("MYSQL_DATABASE"),
	}
}

// Config can be loaded easily by calling func LoadEnvConfig
type Config struct {
	Host     string
	Port     string
	Username string
	Password string // mysql_native_password
	Database string // schema name
}

func (c Config) ToDataSourceURL() string {
	return fmt.Sprintf(
		"%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&parseTime=True&loc=UTC",
		c.Username, c.Password, c.Host, c.Port, c.Database)
}
