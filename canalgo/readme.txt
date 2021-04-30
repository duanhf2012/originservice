功能: mysql修改后,通过binlog实现同步redis. 写的比较随意, canal/redis等的配置没有提取出来. 只做参考

依赖: 基于阿里的 canal开源项目, client连接canal用的是开源的canla-go项目, redis操作则用的是origin里面的RedisModule

使用: 
1. 下载并开启 canal(需要mysql配置canal用户, 具体可以参考canal quick strat 文档)
2. 初始化并启动本服务, 例如:
	dbs.syncRedis = &canalgo.SyncModule{}
	if err := dbs.syncRedis.Init(); err != nil {
		log.Error("init syncRedis failed:%s", err)
		return err
	}
	dbs.syncRedis.Start()
	



