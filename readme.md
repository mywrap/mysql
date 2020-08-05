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
