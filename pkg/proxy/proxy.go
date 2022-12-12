// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy/router"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/wandoulabs/go-zookeeper/zk"
	topo "github.com/wandoulabs/go-zookeeper/zk"
)

type Server struct {
	conf   *Config          // proxy 的配置信息，从配置文件中读取
	topo   *Topology        // 与 zk 或 etcd 的操作接口
	info   models.ProxyInfo // proxy 运行时的一些信息，放在 zk
	groups map[int]int      // ServerGroup 信息，slotid : groupid

	lastActionSeq int // 记录最后一次处理的 action seq，防止重复处理过期 event

	evtbus   chan interface{} // 保存 watch 到的 action 队列
	router   *router.Router   // proxy 与后端 rds 相连的路由信息
	listener net.Listener     // 监听 client 请求

	kill chan interface{} // 优雅退出
	wait sync.WaitGroup   // 等待 proxy 进程退出信号
	stop sync.Once        // 注册 proxy 退出的清理操作
}

func New(addr string, debugVarAddr string, conf *Config) *Server {
	log.Infof("create proxy with config: %+v", conf)

	proxyHost := strings.Split(addr, ":")[0]
	debugHost := strings.Split(debugVarAddr, ":")[0]

	hostname, err := os.Hostname()
	if err != nil {
		log.PanicErrorf(err, "get host name failed")
	}
	if proxyHost == "0.0.0.0" || strings.HasPrefix(proxyHost, "127.0.0.") || proxyHost == "" {
		proxyHost = hostname
	}
	if debugHost == "0.0.0.0" || strings.HasPrefix(debugHost, "127.0.0.") || debugHost == "" {
		debugHost = hostname
	}

	s := &Server{conf: conf, lastActionSeq: -1, groups: make(map[int]int)}

	// ZkFactory 同时支持 zookeeper 和 etcd
	s.topo = NewTopo(conf.productName, conf.zkAddr, conf.fact, conf.provider, conf.zkSessionTimeout)
	s.info.Id = conf.proxyId
	s.info.State = models.PROXY_STATE_OFFLINE
	s.info.Addr = proxyHost + ":" + strings.Split(addr, ":")[1]
	s.info.DebugVarAddr = debugHost + ":" + strings.Split(debugVarAddr, ":")[1]
	s.info.Pid = os.Getpid()
	s.info.StartAt = time.Now().String()
	s.kill = make(chan interface{})

	log.Infof("proxy info = %+v", s.info)

	// 监听客户端请求，socket buffer 未做特殊设置
	if l, err := net.Listen(conf.proto, addr); err != nil {
		log.PanicErrorf(err, "open listener failed")
	} else {
		s.listener = l
	}

	// 创建 Router，初始化 SharedBackendConn map （对后端 redis 的连接）
	// 对 slots 数组进行填充， slot[1]=&Slot{id: 1}，共 1024 个
	s.router = router.NewWithAuth(conf.passwd)
	s.evtbus = make(chan interface{}, 1024)

	// 在 zk 中初始化该 proxy
	s.register()

	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		s.serve() // 服务开始
	}()
	// 在 main 函数中进行 wait 阻塞
	return s
}

func (s *Server) SetMyselfOnline() error {
	log.Info("mark myself online")
	info := models.ProxyInfo{
		Id:    s.conf.proxyId,
		State: models.PROXY_STATE_ONLINE,
	}
	b, _ := json.Marshal(info)
	url := "http://" + s.conf.dashboardAddr + "/api/proxy"
	res, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New("response code is not 200")
	}
	return nil
}

func (s *Server) serve() {
	defer s.close()

	// 等待 proxy 在 zk 中变为 online 状态
	if !s.waitOnline() {
		return
	}

	s.rewatchNodes() // wathc /zk/codis/db_<product_name>/actions

	// fill router 的 slot，分两部分
	// 1) fill Server 中的 groups，Slot id -> Group id
	// 2) fill Router 中的 Slot 中的 bc（根据 zk 信息，重建与后端 redis 的连接）
	for i := 0; i < router.MaxSlotNum; i++ {
		s.fillSlot(i)
	}
	log.Info("proxy is serving")
	go func() {
		defer s.close()
		s.handleConns() // 处理用户发来的请求
	}()

	// 处理 action 的 loop
	s.loopEvents()
}

func (s *Server) handleConns() {
	ch := make(chan net.Conn, 4096)
	defer close(ch)

	go func() {
		for c := range ch { // 拿到一个 client conn，就创建一个 session，不限制数量
			x := router.NewSessionSize(c, s.conf.passwd, s.conf.maxBufSize, s.conf.maxTimeout)
			go x.Serve(s.router, s.conf.maxPipeline)
		}
	}()

	for {
		c, err := s.listener.Accept() // 等待 client 发起 connet，并放到 channel
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.WarnErrorf(err, "[%p] proxy accept new connection failed, get temporary error", s)
				time.Sleep(time.Millisecond * 10)
				continue
			}
			log.WarnErrorf(err, "[%p] proxy accept new connection failed, get non-temporary error, must shutdown", s)
			return
		} else {
			ch <- c
		}
	}
}

func (s *Server) Info() models.ProxyInfo {
	return s.info
}

func (s *Server) Join() {
	s.wait.Wait()
}

func (s *Server) Close() error {
	s.close()
	s.wait.Wait()
	return nil
}

func (s *Server) close() {
	s.stop.Do(func() {
		s.listener.Close()
		if s.router != nil {
			s.router.Close()
		}
		close(s.kill)
	})
}

func (s *Server) rewatchProxy() {
	_, err := s.topo.WatchNode(path.Join(models.GetProxyPath(s.topo.ProductName), s.info.Id), s.evtbus)
	if err != nil {
		log.PanicErrorf(err, "watch node failed")
	}
}

func (s *Server) rewatchNodes() []string {
	nodes, err := s.topo.WatchChildren(models.GetWatchActionPath(s.topo.ProductName), s.evtbus)
	if err != nil {
		log.PanicErrorf(err, "watch children failed")
	}
	return nodes
}

func (s *Server) register() {
	// 在 zk 中注册临时 proxy 节点（标记为 online 状态）和永久 fence 节点
	// fence 节点是为了用来判断 proxy 是否是正常退出，
	// 比如使用 kill -9 杀死 proxy 以后，proxy 节点会消失，fence 节点不会消失，对比下就知道是否正常退出。
	// proxy 在收到 kill 信号(os.Interrupt, syscall.SIGTERM, os.Kill)后会把 proxy fence 和 proxy 节点删除，proxy 也就下线了.
	if _, err := s.topo.CreateProxyInfo(&s.info); err != nil { // /zk/codis/db_%s/proxy/proxy_id
		log.PanicErrorf(err, "create proxy node failed")
	}

	// /zk/codis/db_%s/fence/proxy_id，（不正常退出的话，无法正常启动）
	if _, err := s.topo.CreateProxyFenceNode(&s.info); err != nil && err != zk.ErrNodeExists {
		log.PanicErrorf(err, "create fence node failed")
	}
	log.Warn("********** Attention **********")
	log.Warn("You should use `kill {pid}` rather than `kill -9 {pid}` to stop me,")
	log.Warn("or the node resisted on zk will not be cleaned when I'm quiting and you must remove it manually")
	log.Warn("*******************************")
}

func (s *Server) markOffline() {
	s.topo.Close(s.info.Id)
	s.info.State = models.PROXY_STATE_MARK_OFFLINE
}

func (s *Server) waitOnline() bool {
	for {
		info, err := s.topo.GetProxyInfo(s.info.Id)
		if err != nil {
			log.PanicErrorf(err, "get proxy info failed: %s", s.info.Id)
		}
		switch info.State {
		case models.PROXY_STATE_MARK_OFFLINE:
			log.Infof("mark offline, proxy got offline event: %s", s.info.Id)
			s.markOffline()
			return false
		case models.PROXY_STATE_ONLINE:
			s.info.State = info.State
			log.Infof("we are online: %s", s.info.Id)
			s.rewatchProxy() // watch /zk/codis/db_<proxy_name>/proxy/<proxy_id>
			return true
		}
		select {
		case <-s.kill:
			log.Infof("mark offline, proxy is killed: %s", s.info.Id)
			s.markOffline()
			return false
		default:
		}
		log.Infof("wait to be online: %s", s.info.Id)
		time.Sleep(3 * time.Second)
	}
}

func getEventPath(evt interface{}) string {
	return evt.(topo.Event).Path
}

func needResponse(receivers []string, self models.ProxyInfo) bool {
	var info models.ProxyInfo
	for _, v := range receivers {
		err := json.Unmarshal([]byte(v), &info)
		if err != nil {
			if v == self.Id {
				return true
			}
			return false
		}
		if info.Id == self.Id && info.Pid == self.Pid && info.StartAt == self.StartAt {
			return true
		}
	}
	return false
}

func groupMaster(groupInfo models.ServerGroup) string {
	var master string
	for _, server := range groupInfo.Servers {
		if server.Type == models.SERVER_TYPE_MASTER {
			if master != "" {
				log.Panicf("two master not allowed: %+v", groupInfo)
			}
			master = server.Addr
		}
	}
	if master == "" {
		log.Panicf("master not found: %+v", groupInfo)
	}
	return master
}

func (s *Server) resetSlot(i int) {
	s.router.ResetSlot(i)
}

func (s *Server) fillSlot(i int) {

	// 从 zk 中拿到 slot i 的信息
	slotInfo, slotGroup, err := s.topo.GetSlotByIndex(i)
	if err != nil {
		log.PanicErrorf(err, "get slot by index %d failed", i)
	}

	var from string
	var addr = groupMaster(*slotGroup) // 获取 master（新）地址
	if slotInfo.State.Status == models.SLOT_STATUS_MIGRATE {
		fromGroup, err := s.topo.GetGroup(slotInfo.State.MigrateStatus.From)
		if err != nil {
			log.PanicErrorf(err, "get migrate from failed")
		}
		from = groupMaster(*fromGroup)
		if from == addr {
			log.Panicf("set slot %04d migrate from %s to %s", i, from, addr)
		}
	}

	s.groups[i] = slotInfo.GroupId
	s.router.FillSlot(i, addr, from,
		slotInfo.State.Status == models.SLOT_STATUS_PRE_MIGRATE)
}

func (s *Server) onSlotRangeChange(param *models.SlotMultiSetParam) {
	log.Infof("slotRangeChange %+v", param)
	for i := param.From; i <= param.To; i++ {
		switch param.Status {
		case models.SLOT_STATUS_OFFLINE:
			s.resetSlot(i)
		case models.SLOT_STATUS_ONLINE:
			s.fillSlot(i)
		default:
			log.Panicf("can not handle status %v", param.Status)
		}
	}
}

func (s *Server) onGroupChange(groupId int) {
	log.Infof("group changed %d", groupId)
	for i, g := range s.groups {
		if g == groupId {
			s.fillSlot(i) // 重新填写 group i 的信息
		}
	}
}

func (s *Server) responseAction(seq int64) {
	log.Infof("send response seq = %d", seq)
	err := s.topo.DoResponse(int(seq), &s.info)
	if err != nil {
		log.InfoErrorf(err, "send response seq = %d failed", seq)
	}
}

func (s *Server) getActionObject(seq int, target interface{}) {
	act := &models.Action{Target: target}
	err := s.topo.GetActionWithSeqObject(int64(seq), act)
	if err != nil {
		log.PanicErrorf(err, "get action object failed, seq = %d", seq)
	}
	log.Infof("action %+v", act)
}

func (s *Server) checkAndDoTopoChange(seq int) bool {
	act, err := s.topo.GetActionWithSeq(int64(seq))
	if err != nil { //todo: error is not "not exist"
		log.PanicErrorf(err, "action failed, seq = %d", seq)
	}

	if !needResponse(act.Receivers, s.info) { // no need to response
		return false
	}

	log.Warnf("action %v receivers %v", seq, act.Receivers)

	switch act.Type {
	// PREMIGRATE -> MIGRATE
	case models.ACTION_TYPE_SLOT_MIGRATE, models.ACTION_TYPE_SLOT_CHANGED, // slot migrate 和初始化 action
		models.ACTION_TYPE_SLOT_PREMIGRATE: // slot 要做迁移
		slot := &models.Slot{}
		s.getActionObject(seq, slot)
		s.fillSlot(slot.Id) // 重新 fillslot
	case models.ACTION_TYPE_SERVER_GROUP_CHANGED: // 主从切换 action
		serverGroup := &models.ServerGroup{}
		s.getActionObject(seq, serverGroup)
		s.onGroupChange(serverGroup.Id)
	case models.ACTION_TYPE_SERVER_GROUP_REMOVE:
	//do not care
	case models.ACTION_TYPE_MULTI_SLOT_CHANGED: // setslot action
		param := &models.SlotMultiSetParam{}
		s.getActionObject(seq, param)
		s.onSlotRangeChange(param) // 挨个修正 slot 对应的 backend 信息
	default:
		log.Panicf("unknown action %+v", act)
	}
	return true
}

func (s *Server) processAction(e interface{}) {
	// /zk/codis/db_{product_name}/proxy/
	if strings.Index(getEventPath(e), models.GetProxyPath(s.topo.ProductName)) == 0 {
		info, err := s.topo.GetProxyInfo(s.info.Id) // + proxyid
		if err != nil {
			log.PanicErrorf(err, "get proxy info failed: %s", s.info.Id)
		}
		switch info.State {
		case models.PROXY_STATE_MARK_OFFLINE: // 被标记成 offline 了
			log.Infof("mark offline, proxy got offline event: %s", s.info.Id)
			s.markOffline()
		case models.PROXY_STATE_ONLINE:
			s.rewatchProxy()
		default:
			log.Panicf("unknown proxy state %v", info)
		}
		return
	}

	// re-watch /zk/codis/db_{product_name}/actions
	nodes := s.rewatchNodes()

	seqs, err := models.ExtraSeqList(nodes) // 获得所有的 seq
	if err != nil {
		log.PanicErrorf(err, "get seq list failed")
	}

	if len(seqs) == 0 || !s.topo.IsChildrenChangedEvent(e) {
		return
	}

	// get last pos
	index := -1
	for i, seq := range seqs {
		if s.lastActionSeq < seq {
			index = i
			//break
			//only handle latest action
		}
	}

	if index < 0 {
		return
	}

	actions := seqs[index:] // 获得哪些还没有处理的 seq 对应的 action
	for _, seq := range actions {
		exist, err := s.topo.Exist(path.Join(s.topo.GetActionResponsePath(seq), s.info.Id))
		if err != nil {
			log.PanicErrorf(err, "get action failed")
		}
		if exist {
			continue
		}
		if s.checkAndDoTopoChange(seq) { // 处理 ActionResponse
			s.responseAction(int64(seq))
		}
	}

	s.lastActionSeq = seqs[len(seqs)-1]
}

func (s *Server) loopEvents() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var tick int = 0
	for s.info.State == models.PROXY_STATE_ONLINE { // loop until server 在 zk 里被标记为 offline
		select {
		case <-s.kill: // 收到 kill 信号
			log.Infof("mark offline, proxy is killed: %s", s.info.Id)
			s.markOffline()
		case e := <-s.evtbus:
			evtPath := getEventPath(e)
			log.Infof("got event %s, %v, lastActionSeq %d", s.info.Id, e, s.lastActionSeq)

			// /zk/codis/db_{product_name}/ActionResponse/{seq}
			if strings.Index(evtPath, models.GetActionResponsePath(s.conf.productName)) == 0 {
				seq, err := strconv.Atoi(path.Base(evtPath)) // 拿到 seq
				if err != nil {
					log.ErrorErrorf(err, "parse action seq failed")
				} else {
					if seq < s.lastActionSeq {
						log.Infof("ignore seq = %d", seq)
						continue
					}
				}
			}
			s.processAction(e)
		case <-ticker.C: // 探活，将 ping 命令插入到正常的请求中
			if maxTick := s.conf.pingPeriod; maxTick != 0 { // pingPeriod 秒 发一次 ping
				if tick++; tick >= maxTick {
					s.router.KeepAlive()
					tick = 0
				}
			}
		}
	}
}
