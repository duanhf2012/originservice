package canalgo

import (
	"fmt"
	"github.com/duanhf2012/origin/service"
	"github.com/duanhf2012/origin/sysmodule/redismodule"
	"github.com/golang/protobuf/proto"
	"github.com/withlin/canal-go/client"
	pbe "github.com/withlin/canal-go/protocol/entry"
	"log"
	"time"
)

func init() {

}

type SyncModule struct {
	connector *client.SimpleCanalConnector
	redis *redismodule.RedisModule


	service.Module
}


func (s *SyncModule) Init() error  {
	// 创建与Canal的连接
	connector := client.NewSimpleCanalConnector("127.0.0.1", 11111, "", "", "example", 60000, 60*60*1000)
	err := connector.Connect()
	if err != nil {
		return err
	}

	// 订阅哪些类型的消息
	// https://github.com/alibaba/canal/wiki/AdminGuide
	//mysql 数据解析关注的表，Perl正则表达式.
	//
	//多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)
	//
	//常见例子：
	//
	//  1.  所有表：.*   or  .*\\..*
	//	2.  canal schema下所有表： canal\\..*
	//	3.  canal下的以canal打头的表：canal\\.canal.*
	//	4.  canal schema下的一张表：canal\\.test1
	//  5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
	err = connector.Subscribe(".*\\..*")
	if err != nil {
		log.Println(err)
		return err
	}
	s.connector = connector

	// 创建Redis连接
	var redisCfg redismodule.ConfigRedis
	redisCfg.DbIndex = 0         //数据库索引
	redisCfg.IdleTimeout = 1000 //最大的空闲连接等待时间，超过此时间后，空闲连接将被关闭
	redisCfg.MaxIdle = 100      //最大的空闲连接数，表示即使没有redis连接时依然可以保持N个空闲的连接，而不被清除，随时处于待命状态。
	redisCfg.IdleTimeout = 100  //最大的空闲连接等待时间，超过此时间后，空闲连接将被关闭
	redisCfg.IP = "127.0.0.1"   //redis服务器IP
	redisCfg.Port = 6379      //redis服务器端口
	s.redis = &redismodule.RedisModule{}
	s.redis.Init(&redisCfg)

	return nil
}

func (s *SyncModule) Start() {

	go func(){
		defer func(){
			err := recover()
			if err != nil {
				log.Println("error in canalgo:", err)
			}
		}()

		for {
			message, err := s.connector.Get(10, nil, nil)
			if err != nil {
				log.Println(err)
				// 重连
				s.connector.DisConnection()
				s.connector.Running = false
				s.connector.Connect()
				time.Sleep(1000*time.Millisecond)
				continue
			}
			batchId := message.Id
			if batchId == -1 || len(message.Entries) <= 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			log.Println("get sys msg, batchid:", batchId)
			s.processEntries(message.Entries)
		}
	}()
}



func (s *SyncModule) processEntries(entries []pbe.Entry) {
	for _, entry := range entries {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)

		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		if err!=nil{
			return
		}

		if rowChange != nil {
			eventType := rowChange.GetEventType()
			header := entry.GetHeader()
			//fmt.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))
			//dbname := header.GetSchemaName()
			tablename := header.GetTableName()
			for _, rowData := range rowChange.GetRowDatas() {
				switch eventType {
				case pbe.EventType_DELETE:
					key, _ := getHashKey(tablename, rowData.GetBeforeColumns())
					if key != "" {
						s.redis.DelHash(key)
					}
				case pbe.EventType_INSERT:
					key, _ := getHashKey(tablename, rowData.GetAfterColumns())
					fmt.Println("insert key:", key)
					for _, col := range rowData.GetAfterColumns() {
						s.redis.SetHash(key, col.Name, col.Value)
					}
				case pbe.EventType_UPDATE:
					key, _ := getHashKey(tablename, rowData.GetBeforeColumns())
					fmt.Println("update key:", key)

					if exist, _ := s.redis.ExistsKey(key); exist{
						for _, col := range rowData.GetAfterColumns() {
							if col.GetUpdated() {
								s.redis.SetHash(key, col.Name, col.Value)
							}
						}
					}else{
						for _, col := range rowData.GetAfterColumns() {
							s.redis.SetHash(key, col.Name, col.Value)
						}
					}
				}
			}
		}
	}
}
// 获得hash key, 可根据表的具体情况修改
// 本项目目前设计每张表都有ID主键
func getHashKey(tabName string, cols []*pbe.Column) (string, error) {
	for _, col := range cols {
		if col.GetName() == "id" {
			return tabName+":"+col.GetValue(), nil
		}
	}
	return "", nil
}