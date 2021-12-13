package mysql

import (
	cryptoRand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
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
			DoUpdates: clause.AssignmentColumns([]string{
				"last_modified", "phone"}),
		}).
		Create(clients)
	if qr.Error != nil {
		return fmt.Errorf("error create clients upsert: %v", qr.Error)
	}
	return nil
}

func (r myRepo) ReadClient(username string) (testClient, error) {
	ret := testClient{Username: username}
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

// read last project of input username
func (r myRepo) ReadProject(username string) (testProject, error) {
	qrJoin := r.DB
	qrJoin = qrJoin.Where(&testProject{ClientUsername: username}).Order("id DESC")
	qrJoin = qrJoin.Preload("Client")
	var row testProject
	ret := qrJoin.First(&row) // can return record not found
	return row, ret.Error
}

// data definition: table test_clients
type testClient struct {
	Username        string `gorm:"type:varchar(191);primary_key"`
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

	ClientUsername string      // magic field name foreign key
	Client         *testClient `gorm:"constraint:testClient,OnUpdate:NO ACTION,OnDelete:NO ACTION;"`

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

	mErr := repo0.DB.Debug().AutoMigrate(&testClient{})
	if mErr != nil {
		t.Error(mErr)
	}
	mErr = repo0.DB.Debug().AutoMigrate(&testProject{})
	if mErr != nil {
		t.Error(mErr)
	}

	// insert and select join via gorm

	user1 := "tungdt"
	cli1 := testClient{Username: user1, Phone: "84396xxx527",
		FullName: "Đào Thanh Tùng", DateOfBirth: "1991-08-20"}
	repo0.CreateClient(cli1)

	repo0.CreateClient(testClient{Username: "landt",
		FullName: "Đào Thị Lán", Phone: "097xxx8543"})

	repo0.CreateProject(testProject{Name: "prj1: forget ",
		ClientUsername: user1, Deadline: time.Unix(0, 0)})
	repo0.CreateProject(testProject{Name: "prj2", ClientUsername: user1,
		Deadline: time.Unix(0, 0), ValueVNDs: 30000})

	err := repo0.CreateProject(testProject{Name: "prj3",
		ClientUsername: "invalidCli", Deadline: time.Unix(0, 0)})
	if err == nil ||
		!strings.Contains(err.Error(), "constraint fail") {
		t.Errorf("real: %v, expected: foreign key error", err)
	}

	row, err := repo0.ReadProject(user1)
	if err != nil {
		t.Errorf("error ReadProject: %v", err)
	}
	t.Logf("read project: client: %#v, project: %#v", row.Client, row)
	if row.Client.DateOfBirth != cli1.DateOfBirth {
		t.Errorf("join row: real: %v, expected: %v",
			row.Client.DateOfBirth, cli1.DateOfBirth)
	}

	// update table has foreign key

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
		SELECT username, full_name, phone
		FROM test_clients LIMIT 2;
	`)
	if err != nil {
		t.Fatalf("error Query: %v", err)
	}
	defer rows.Close()
	nRows := 0
	for rows.Next() {
		nRows += 1
		var full_name, username, phone string
		err = rows.Scan(&username, &full_name, &phone)
		if err != nil {
			t.Errorf("error rows Scan: %v", err)
			continue
		}
		t.Logf("client: %v, %v, %v", username, full_name, phone)
	}
	if nRows < 2 {
		t.Errorf("nRows: real: %v, expected: 2", nRows)
	}
}
func genUUID() string {
	b := make([]byte, 16)
	_, _ = cryptoRand.Read(b)
	r := hex.EncodeToString(b)
	return r
}

func TestGormV2(t *testing.T) {
	dupCli := testClient{
		Username:     "testBatchDupKey",
		LastModified: time.Now().Format(time.RFC3339Nano),
	}
	repo0.CreateClient(dupCli)
	nAffecteds, err := repo0.CreateClientsIgnore([]testClient{
		{Username: "testBatchId00_" + genUUID()},
		dupCli,
		{Username: "testBatchId02_" + genUUID()},
	})
	if err != nil {
		t.Error(err)
	}
	if nAffecteds != 2 {
		t.Error(err)
	}

	dupCli2 := testClient{
		Username:     "testBatchDupIdUpsert",
		Phone:        "old",
		LastModified: time.Now().Format(time.RFC3339Nano),
	}
	repo0.CreateClient(dupCli2)
	newValDupCli2 := time.Now().Format(time.RFC3339Nano) + "new"
	err2 := repo0.CreateClientsUpsert([]testClient{
		{Username: "testBatchId10_" + genUUID()},
		{Username: dupCli2.Username, Phone: "old", LastModified: newValDupCli2},
		{Username: "testBatchId12_" + genUUID()},
	})
	if err2 != nil {
		t.Error(err2)
	}
	loadedCli2, err := repo0.ReadClient(dupCli2.Username)
	if err != nil {
		t.Error(err)
	}
	if loadedCli2.LastModified != newValDupCli2 {
		t.Error("batch upsert fail: not update")
	}
	if loadedCli2.Phone != "old" {
		t.Error("batch upsert fail: update wrong column")
	}
}

type testGift struct {
	Id       int64     `gorm:"primary_key;AUTO_INCREMENT"`
	GiftType string    `gorm:"type:varchar(191);index:idx_priority"`
	IsUsed   bool      `gorm:"index:idx_priority"`
	CreateAt time.Time `gorm:"index:idx_priority"`
	UsedAt   time.Time
}

type testGiftSummary struct {
	GiftType   string `gorm:"primary_key;type:varchar(191)"`
	NUsedGifts int64
}

// takeAGift is a transaction that updates a gift field `is_used` and update the
// number of used gifts in another table
func takeAGift(giftType string, isDebug bool) (giftId int64, retErr error) {
	db := repo0.DB
	if isDebug {
		db = db.Debug()
	}
	tx := db.Begin(&sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	if tx.Error != nil {
		return 0, fmt.Errorf("beginTx: %v", tx.Error)
	}
	var takenGift testGift
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Table("test_gifts").
		Where(map[string]interface{}{"gift_type": giftType, "is_used": false}).
		Order("create_at ASC").
		First(&takenGift).Error
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("select gift: %v", err)
	}
	takenGift.IsUsed, takenGift.UsedAt = true, time.Now()
	err = tx.Save(&takenGift).Error
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("update gift: %v", err)
	}
	var summary testGiftSummary
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(&testGiftSummary{GiftType: giftType}).
		First(&summary).Error
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("select summary: %v", err)
	}
	summary.NUsedGifts += 1
	err = tx.Save(&summary).Error
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("update summary: %v", err)
	}
	err = tx.Commit().Error
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("commit: %v", err)
	}
	return takenGift.Id, nil
}

func TestGormTransaction(t *testing.T) {
	// create table if needed
	if err := repo0.DB.AutoMigrate(&testGift{}); err != nil {
		t.Fatal(err)
	}
	if err := repo0.DB.AutoMigrate(&testGiftSummary{}); err != nil {
		t.Fatal(err)
	}
	//truncate
	db := repo0.DB.Debug()
	if err := db.Where("1=1").Delete(&testGift{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("1=1").Delete(&testGiftSummary{}).Error; err != nil {
		t.Fatal(err)
	}
	//
	const giftType0 = "giftType0"
	const nGifts = 500
	rand.Seed(time.Now().UnixNano())
	gifts := make([]testGift, nGifts)
	for i := 0; i < nGifts; i++ {
		gifts[i] = testGift{
			Id: int64(i + 1), GiftType: giftType0, IsUsed: false,
			CreateAt: time.Now(),
			UsedAt:   time.Unix(0, 0)}
	}
	qr := repo0.DB.Create(gifts)
	if qr.Error != nil || qr.RowsAffected != nGifts {
		t.Fatalf("error create gifts: %v, %v", qr.Error, qr.RowsAffected)
	}
	t.Logf("created %v gifts", nGifts)
	if err := db.Create(&testGiftSummary{GiftType: giftType0}).Error; err != nil {
		t.Fatalf("error create summary: %v", err)
	}

	t.Log("--------------------------------------------------------------")
	const nGiftsToTake = 200
	wg := &sync.WaitGroup{}
	takenGifts := make(map[int64]bool)
	mutex := &sync.Mutex{}
	for i := 0; i < nGiftsToTake; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Add(-1)
			giftId, err := takeAGift(giftType0, i == 100) // only print query debug for gift id 100
			if err != nil {
				t.Logf("failed takeAGift: %v", err)
				return
			}
			mutex.Lock()
			takenGifts[giftId] = true
			mutex.Unlock()
		}(i)
	}
	wg.Wait()

	nUsedsByCountMemory := int64(len(takenGifts))
	nUsedsByCountTable := int64(0)
	stdSqlDB, _ := repo0.DB.DB()
	row := stdSqlDB.QueryRow(`SELECT COUNT(*) FROM test_gifts WHERE is_used=true`)
	err := row.Scan(&nUsedsByCountTable)
	if err != nil {
		t.Errorf("error Scan count: %v", err)
	}
	var tmp testGiftSummary
	if err := db.First(
		&testGiftSummary{GiftType: giftType0}).First(&tmp).Error; err != nil {
		t.Errorf("error count used gifts: %v", err)
	}
	nUsedsBySummaryTable := tmp.NUsedGifts

	t.Logf("nTries %v, nUsedsByCountMemory %v, nUsedsByCountTable %v, nUsedsBySummaryTable %v",
		nGiftsToTake, nUsedsByCountMemory, nUsedsByCountTable, nUsedsBySummaryTable)
	// nUsedsByCountTable must equal to nUsedsByCountMemory and
	// nUsedsBySummaryTable (but can be smaller than nTries because
	// some transaction fail and roll back)
	if nUsedsByCountTable != nUsedsByCountMemory ||
		nUsedsByCountTable != nUsedsBySummaryTable {
		t.Errorf("error inconsistent nTakenGifts: nUsedsByCountMemory %v, nUsedsByCountTable %v, nUsedsBySummaryTable %v",
			nUsedsByCountMemory, nUsedsByCountTable, nUsedsBySummaryTable)
	}
}
