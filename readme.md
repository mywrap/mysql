# MySQL client

Quick config to connect MySQL from environment vars.  
Wrapped [go-sql-driver/mysql](
https://github.com/daominah/gomicrokit) and [jinzhu/gorm](https://github.com/jinzhu/gorm).

## Usage

````go
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
// log: error create testClient: Error 1062: Duplicate entry for 
// key 'test_clients.PRIMARY'

````
Detail in [mysql_test.go](./mysql_test.go).

## Useful commands

````mysql
CREATE DATABASE `database0` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

SELECT `Host`, `User`, `plugin`, `Super_priv` FROM mysql.user;
````
