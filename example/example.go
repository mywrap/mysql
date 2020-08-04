package main

import (
	"log"

	"github.com/mywrap/mysql"
)

func main() {
	// see mysql_test.go for detail

	// envs: MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
	db, err := mysql.ConnectViaGORM(mysql.LoadEnvConfig())
	if err != nil {
		log.Fatalf("error ConnectViaGORM: %v", err)
	}
	qr := db.Create(testClient{
		FullName: "Đào Thị Lán",
		Phone:    "09xxx28543",
		Id:       "12a535ea-a105-452d-b457-8c3bb66a4d25",
	})
	if qr.Error != nil {
		log.Printf("error create testClient: %v", qr.Error)
	}
}

type testClient struct {
	Id              string `gorm:"type:varchar(40);primary_key"`
	Username        string `gorm:"type:varchar(191);unique_index"`
	HashedPassword  string `json:"-"`
	Phone           string `gorm:"type:varchar(191)"`
	Email           string `gorm:"type:varchar(191)"`
	IsVerifiedEmail bool
	FullName        string
	DateOfBirth     string // YYYY-MM-DD
}
