package mysql

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// These test need a MySQL server to run

// create a struct that wraps database client so we can add methods to it
type myRepo struct{ DB *gorm.DB }

func (r myRepo) CreateClient(cli testClient) error {
	qr := r.DB.Create(&cli)
	if qr.Error != nil {
		return fmt.Errorf("error create client: %v", qr.Error)
	}
	return nil
}

// batch insert, ignore duplicate key
func (r myRepo) CreateClientsIgnore(clients []testClient) (int64, error) {
	qr := r.DB.Debug().
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(clients)
	if qr.Error != nil {
		return 0, fmt.Errorf("error create clients: %v", qr.Error)
	}
	return qr.RowsAffected, nil
}

// batch insert, update duplicate key
func (r myRepo) CreateClientsUpsert(clients []testClient) error {
	qr := r.DB.Debug().
		Clauses(clause.OnConflict{
			DoUpdates: clause.AssignmentColumns([]string{"last_modified"}),
		}).
		Create(clients)
	if qr.Error != nil {
		return fmt.Errorf("error create clients upsert: %v", qr.Error)
	}
	return nil
}

func (r myRepo) ReadClient(id string) (testClient, error) {
	ret := testClient{Id: id}
	qr := r.DB.First(&ret)
	return ret, qr.Error
}

func (r myRepo) CreateProject(prj testProject) error {
	qr := r.DB.Create(&prj)
	if qr.Error != nil {
		return fmt.Errorf("error create project: %v", qr.Error)
	}
	return nil
}

func (r myRepo) ReadProject(clientId string) (testProject, error) {
	qrJoin := r.DB
	qrJoin = qrJoin.Where(&testProject{ClientId: clientId}).Order("id ASC")
	qrJoin = qrJoin.Preload("Client")
	var row testProject
	ret := qrJoin.First(&row) // can return record not found
	return row, ret.Error
}

// data definition: table test_clients
type testClient struct {
	Id              string `gorm:"type:varchar(191);primary_key"`
	Username        string `gorm:"type:varchar(191);unique_index"`
	HashedPassword  string `json:"-"`
	Phone           string `gorm:"type:varchar(191)"`
	Email           string `gorm:"type:varchar(191)"`
	IsVerifiedEmail bool
	FullName        string
	DateOfBirth     string // YYYY-MM-DD
	LastModified    string
}

// data definition: table test_projects
type testProject struct {
	Id int64 `gorm:"primary_key;AUTO_INCREMENT"`

	ClientId string
	Client   *testClient `gorm:"constraint:ClientId,OnUpdate:NO ACTION,OnDelete:NO ACTION;"`

	Name      string
	Address   string
	ValueVNDs float64
	Deadline  time.Time

	Files       string `gorm:"type:varchar(2047)"` // a jsoned array
	Description string `gorm:"type:mediumtext"`    // max 16 MB
}

// shared client for all test functions
var repo0 myRepo

func TestMain(m *testing.M) {
	db, err := ConnectViaGORM(LoadEnvConfig())
	if err != nil {
		panic(fmt.Sprintf("error ConnectViaGORM: %v", err))
	}
	repo0 = myRepo{DB: db}
	os.Exit(m.Run())
}

func TestConfig_ToDataSourceURL(t *testing.T) {
	cfg := LoadEnvConfig()
	t.Logf(cfg.ToDataSourceURL())
}

func TestMysql(t *testing.T) {
	// init: create tables

	mErr := repo0.DB.AutoMigrate(&testClient{})
	if mErr != nil {
		t.Error(mErr)
	}
	mErr = repo0.DB.AutoMigrate(&testProject{})
	if mErr != nil {
		t.Error(mErr)
	}

	// insert and select join via gorm

	cid1 := "e6b84aa6-83ed-4a9d-8f41-c43a2da03e85"
	cli1 := testClient{Id: cid1, FullName: "Đào Thanh Tùng", Username: "tungdt"}
	repo0.CreateClient(cli1)
	repo0.CreateClient(testClient{FullName: "Đào Thị Lán", Phone: "097xxx8543",
		Id: "12a535ea-a105-452d-b457-8c3bb66a4d25"})
	repo0.CreateProject(testProject{Name: "prj1: marry cli1", ClientId: cid1,
		Deadline: time.Unix(0, 0)})
	repo0.CreateProject(testProject{Name: "prj2", ClientId: cid1,
		Deadline: time.Unix(0, 0), ValueVNDs: 30000})

	err := repo0.CreateProject(testProject{Name: "prj3", ClientId: "invalidCliId",
		Deadline: time.Unix(0, 0)})
	if err == nil ||
		!strings.Contains(err.Error(), "constraint fail") {
		t.Errorf("real: %v, expected: foreign key error", err)
	}

	row, err := repo0.ReadProject(cid1)
	if row.Client.Username != cli1.Username {
		t.Errorf("join row: real: %v, expected: %v",
			row.Client.Username, cli1.Username)
	}

	// update table has foreign key

	t.Logf("project: %#v", row)
	row.Description = "updated description at " + time.Now().Format(time.RFC3339)
	row.Client = nil
	qr := repo0.DB.Debug().Save(&row)
	if qr.Error != nil {
		t.Error(qr.Error)
	}

	// select via official Go database/sql

	client2, err := Connect(LoadEnvConfig())
	if err != nil {
		t.Errorf("error Connect: %v", err)
	}
	rows, err := client2.Query(`
		SELECT id, full_name, username, phone
		FROM test_clients LIMIT 2;
	`)
	if err != nil {
		t.Fatalf("error Query: %v", err)
	}
	defer rows.Close()
	nRows := 0
	for rows.Next() {
		nRows += 1
		var id, full_name, username, phone string
		err = rows.Scan(&id, &full_name, &username, &phone)
		if err != nil {
			t.Errorf("error rows Scan: %v", err)
			continue
		}
		t.Logf("client: %v, %v, %v, %v",
			id, full_name, username, phone)
	}
	if nRows < 2 {
		t.Errorf("nRows: real: %v, expected: 2", nRows)
	}
}
func genUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	r := hex.EncodeToString(b)
	return r
}

func TestGormV2(t *testing.T) {
	dupCli := testClient{
		Id:           "testBatchDupId",
		LastModified: time.Now().String(),
	}
	repo0.CreateClient(dupCli)
	nAffecteds, err := repo0.CreateClientsIgnore([]testClient{
		{Id: "testBatchId0_" + genUUID()},
		dupCli,
		{Id: "testBatchId2_" + genUUID()},
	})
	if err != nil {
		t.Error(err)
	}
	if nAffecteds != 2 {
		t.Error(err)
	}

	dupCli2 := testClient{
		Id:           "testBatchDupIdUpsert",
		LastModified: "oldValue",
	}
	repo0.CreateClient(dupCli2)
	newValDupCli2 := time.Now().Format(time.RFC3339Nano)
	err2 := repo0.CreateClientsUpsert([]testClient{
		{Id: "testBatchId10_" + genUUID()},
		{Id: dupCli2.Id, LastModified: newValDupCli2},
		{Id: "testBatchId12_" + genUUID()},
	})
	if err2 != nil {
		t.Error(err2)
	}
	loadedCli2, err := repo0.ReadClient(dupCli2.Id)
	if err != nil {
		t.Error(err)
	}
	if loadedCli2.LastModified != newValDupCli2 {
		t.Error("batch upsert fail")
	}
}
