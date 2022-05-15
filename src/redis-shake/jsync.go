package run

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/alibaba/RedisShake/pkg/libs/atomic2"
	"github.com/alibaba/RedisShake/pkg/libs/io/pipe"
	"github.com/alibaba/RedisShake/pkg/libs/log"
	"github.com/alibaba/RedisShake/pkg/redis"
	"github.com/alibaba/RedisShake/redis-shake/base"
	utils "github.com/alibaba/RedisShake/redis-shake/common"
	conf "github.com/alibaba/RedisShake/redis-shake/configure"
	"github.com/alibaba/RedisShake/redis-shake/heartbeat"
	"github.com/alibaba/RedisShake/redis-shake/metric"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// main struct
type CmdJSync struct {
	jdbSyncers []*jdbSyncer
}

// return send buffer length, delay channel length, target db offset
func (cmd *CmdJSync) GetDetailedInfo() interface{} {
	ret := make([]map[string]interface{}, len(cmd.jdbSyncers))
	for i, syncer := range cmd.jdbSyncers {
		if syncer == nil {
			continue
		}
		ret[i] = syncer.GetExtraInfo()
	}
	return ret
}

func (cmd *CmdJSync) Main() {
	type syncNode struct {
		id             int
		source         string
		sourcePassword string
	}

	// source redis number
	total := utils.GetTotalLink()
	fmt.Println(conf.Options.SourceAddressList)
	syncChan := make(chan syncNode, total)
	cmd.jdbSyncers = make([]*jdbSyncer, total)
	for i, source := range conf.Options.SourceAddressList {
		nd := syncNode{
			id:             i,
			source:         source,
			sourcePassword: conf.Options.SourcePasswordRaw,
		}
		syncChan <- nd
	}

	var wg sync.WaitGroup
	wg.Add(len(conf.Options.SourceAddressList))

	for i := 0; i < conf.Options.SourceRdbParallel; i++ {
		go func() {
			for {
				nd, ok := <-syncChan
				if !ok {
					break
				}

				ds := NewjdbSyncer(nd.id, nd.source, nd.sourcePassword,
					conf.Options.HttpProfile+i)
				cmd.jdbSyncers[nd.id] = ds
				log.Infof("routine[%v] starts syncing data from %v with http[%v]",
					ds.id, ds.source, ds.httpProfilePort)
				// run in routine
				go ds.sync()

				// wait full sync done
				<-ds.waitFull

				wg.Done()
			}
		}()
	}

	wg.Wait()
	close(syncChan)

	// never quit because increment syncing is still running
	select {}
}

/*------------------------------------------------------*/
// one sync link corresponding to one jdbSyncer
func NewjdbSyncer(id int, source, sourcePassword string, httpPort int) *jdbSyncer {
	ds := &jdbSyncer{
		id:              id,
		source:          source,
		sourcePassword:  sourcePassword,
		httpProfilePort: httpPort,
		waitFull:        make(chan struct{}),
	}

	// add metric
	metric.AddMetric(id)

	return ds
}

type jdbSyncer struct {
	id int // current id in all syncer

	source         string // source address
	sourcePassword string // source password

	httpProfilePort int // http profile port

	// metric info
	rbytes, wbytes, nentry, ignore atomic2.Int64
	forward, nbypass               atomic2.Int64
	targetOffset                   atomic2.Int64

	sendBuf  chan cmdDetail // sending queue
	waitFull chan struct{}  // wait full sync done
}

func (ds *jdbSyncer) GetExtraInfo() map[string]interface{} {
	return map[string]interface{}{
		"SourceAddress":  ds.source,
		"SenderBufCount": len(ds.sendBuf),
		"TargetDBOffset": ds.targetOffset.Get(),
	}
}

func (ds *jdbSyncer) Stat() *syncerStat {
	return &syncerStat{
		rbytes: ds.rbytes.Get(),
		wbytes: ds.wbytes.Get(),
		nentry: ds.nentry.Get(),
		ignore: ds.ignore.Get(),

		forward: ds.forward.Get(),
		nbypass: ds.nbypass.Get(),
	}
}

func (ds *jdbSyncer) sync() {

	base.Status = "waitfull"
	var input io.ReadCloser
	var nsize int64
	if conf.Options.Psync {
		input, nsize = ds.sendPSyncCmd(ds.source, conf.Options.SourceAuthType, ds.sourcePassword, conf.Options.SourceTLSEnable, conf.Options.SourceTLSSkipVerify)
	} else {
		input, nsize = ds.sendSyncCmd(ds.source, conf.Options.SourceAuthType, ds.sourcePassword, conf.Options.SourceTLSEnable, conf.Options.SourceTLSSkipVerify)
	}
	defer input.Close()

	log.Infof("jdbSyncer[%v] rdb file size = %d\n", ds.id, nsize)

	// start heartbeat
	if len(conf.Options.HeartbeatUrl) > 0 {
		heartbeatCtl := heartbeat.HeartbeatController{
			ServerUrl: conf.Options.HeartbeatUrl,
			Interval:  int32(conf.Options.HeartbeatInterval),
		}
		go heartbeatCtl.Start()
	}

	reader := bufio.NewReaderSize(input, utils.ReaderBufferSize)
	name := fmt.Sprintf("%s.rdb", conf.Options.Id)
	dumpto := utils.OpenWriteFile(name)
	defer dumpto.Close()

	writer := bufio.NewWriterSize(dumpto, utils.WriterBufferSize)
	// dump rdb
	ds.dumpRDBFile(reader, writer, nsize)

	timestamp := time.Now().Format("2006-01-02T15-04-05.000")
	newname := fmt.Sprintf("%s-%s.rdb", conf.Options.Id, timestamp)
	if err := os.Rename(name, newname); err != nil {
		log.PanicErrorf(err, "rename rdb file failed")
	}

	// sync increment
	base.Status = "incr"
	close(ds.waitFull)
	ds.syncCommand(reader)
}

func (ds *jdbSyncer) sendSyncCmd(master, auth_type, passwd string, tlsEnable, tlsSkipVerify bool) (net.Conn, int64) {
	c, wait := utils.OpenSyncConn(master, auth_type, passwd, tlsEnable, tlsSkipVerify)
	for {
		select {
		case nsize := <-wait:
			if nsize == 0 {
				log.Infof("jdbSyncer[%v] + waiting source rdb", ds.id)
			} else {
				return c, nsize
			}
		case <-time.After(time.Second):
			log.Infof("jdbSyncer[%v] - waiting source rdb", ds.id)
		}
	}
}

func (ds *jdbSyncer) sendPSyncCmd(master, auth_type, passwd string, tlsEnable, tlsSkipVerify bool) (pipe.Reader, int64) {
	c := utils.OpenNetConn(master, auth_type, passwd, tlsEnable, tlsSkipVerify)
	log.Infof("jdbSyncer[%v] psync connect '%v' with auth type[%v] OK!", ds.id, master, auth_type)

	// utils.SendPSyncListeningPort(c, conf.Options.HttpProfile)
	// log.Infof("jdbSyncer[%v] psync send listening port[%v] OK!", ds.id, conf.Options.HttpProfile)

	// reader buffer bind to client
	br := bufio.NewReaderSize(c, utils.ReaderBufferSize)
	// writer buffer bind to client
	bw := bufio.NewWriterSize(c, utils.WriterBufferSize)

	log.Infof("jdbSyncer[%v] try to send 'psync' command", ds.id)
	// send psync command and decode the result
	runid, offset, wait := utils.SendPSyncFullsync(br, bw)
	ds.targetOffset.Set(offset)
	log.Infof("jdbSyncer[%v] psync runid = %s offset = %d, fullsync", ds.id, runid, offset)

	// get rdb file size
	var nsize int64
	for nsize == 0 {
		select {
		case nsize = <-wait:
			if nsize == 0 {
				log.Infof("jdbSyncer[%v] +", ds.id)
			}
		case <-time.After(time.Second):
			log.Infof("jdbSyncer[%v] -", ds.id)
		}
	}

	// write -> pipew -> piper -> read
	piper, pipew := pipe.NewSize(utils.ReaderBufferSize)

	go func() {
		defer pipew.Close()
		p := make([]byte, 8192)
		// read rdb in for loop
		for rdbsize := int(nsize); rdbsize != 0; {
			// br -> pipew
			rdbsize -= utils.Iocopy(br, pipew, p, rdbsize)
		}
		// woc测试下来这个for循环就没走到啊
		for {
			/*
			 * read from br(source redis) and write into pipew.
			 * Generally speaking, this function is forever run.
			 */
			n, err := ds.pSyncPipeCopy(c, br, bw, offset, pipew)
			log.Infof("jdbSyncer[%v] psync runid = %s offset = %d, nowsync2", ds.id, runid, offset)
			if err != nil {
				utils.OpLogRotater.Rotate()
				log.PanicErrorf(err, "jdbSyncer[%v] psync runid = %s, offset = %d, pipe is broken",
					ds.id, runid, offset)
			}
			// the 'c' is closed every loop

			offset += n
			ds.targetOffset.Set(offset)

			log.Infof("jdbSyncer[%v] psync runid = %s offset = %d, nowsync3", ds.id, runid, offset)
			// reopen 'c' every time
			for {
				// ds.SyncStat.SetStatus("reopen")
				base.Status = "reopen"
				time.Sleep(time.Second)
				c = utils.OpenNetConnSoft(master, auth_type, passwd, tlsEnable)
				if c != nil {
					// log.PurePrintf("%s\n", NewLogItem("SourceConnReopenSuccess", "INFO", LogDetail{Info: strconv.FormatInt(offset, 10)}))
					log.Infof("jdbSyncer[%v] Event:SourceConnReopenSuccess\tId: %s\toffset = %d",
						ds.id, conf.Options.Id, offset)
					// ds.SyncStat.SetStatus("incr")
					base.Status = "incr"
					break
				} else {
					// log.PurePrintf("%s\n", NewLogItem("SourceConnReopenFail", "WARN", NewErrorLogDetail("", "")))
					log.Errorf("jdbSyncer[%v] Event:SourceConnReopenFail\tId: %s", ds.id, conf.Options.Id)
				}
			}

			if role, err := utils.GetRedisRole(master, auth_type, passwd, tlsEnable, tlsSkipVerify); err != nil {
				utils.OpLogRotater.Rotate()
				log.PanicErrorf(err, "get target redis role failed")
			} else {
				log.Infof("jdbSyncer[%v] get target redis role: %s", ds.id, role)
				if strings.ToLower(role) != "slave" {
					utils.OpLogRotater.Rotate()
					log.Panicf("target redis role is not slave")
				}
			}
			utils.AuthPassword(c, auth_type, passwd)
			// utils.SendPSyncListeningPort(c, conf.Options.HttpProfile)
			br = bufio.NewReaderSize(c, utils.ReaderBufferSize)
			bw = bufio.NewWriterSize(c, utils.WriterBufferSize)
			utils.SendPSyncContinue(br, bw, runid, offset, true)
		}
	}()
	return piper, nsize
}

func (ds *jdbSyncer) pSyncPipeCopy(c net.Conn, br *bufio.Reader, bw *bufio.Writer, offset int64, copyto io.Writer) (int64, error) {
	var nread atomic2.Int64
	go func() {
		defer c.Close()
		for range time.NewTicker(1 * time.Second).C {
			select {
			case <-ds.waitFull:
				if err := utils.SendPSyncAck(bw, offset+nread.Get()); err != nil {
					log.Errorf("jdbSyncer[%v] send offset to source redis failed[%v]", ds.id, err)
					return
				}
			default:
				if err := utils.SendPSyncAck(bw, 0); err != nil {
					log.Errorf("jdbSyncer[%v] send offset to source redis failed[%v]", ds.id, err)
					return
				}
			}
		}
		log.Errorf("jdbSyncer[%v] heartbeat thread closed!", ds.id)
	}()

	var p = make([]byte, 8192)
	for {
		n, err := br.Read(p)
		if err != nil {
			return nread.Get(), nil
		}
		if _, err := copyto.Write(p[:n]); err != nil {
			return nread.Get(), err
		}
		nread.Add(int64(n))
	}
}

func (ds *jdbSyncer) dumpRDBFile(reader *bufio.Reader, writer *bufio.Writer, nsize int64) {
	var nread atomic2.Int64
	wait := make(chan struct{})

	// read from reader and write into writer int stream way
	go func() {
		defer close(wait)
		p := make([]byte, utils.WriterBufferSize)
		for nsize != nread.Get() {
			nstep := int(nsize - nread.Get())
			ncopy := int64(utils.Iocopy(reader, writer, p, nstep))
			nread.Add(ncopy)
			utils.FlushWriter(writer)
		}
	}()

	// print stat
	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		n := nread.Get()
		p := 100 * n / nsize
		log.Infof("routine[%v] total = %s - %12s [%3d%%]\n", ds.id, utils.GetMetric(nsize), utils.GetMetric(n), p)
	}
	log.Infof("routine[%v] dump: rdb done", ds.id)
}

func (ds *jdbSyncer) syncCommand(reader *bufio.Reader) {

	ds.sendBuf = make(chan cmdDetail, conf.Options.SenderCount)
	var sendId atomic2.Int64

	go func() {
		var (
			scmd string
			argv [][]byte
			err  error
		)

		decoder := redis.NewDecoder(reader)

		log.Infof("jdbSyncer[%v] FlushEvent:IncrSyncStart\tId:%s\t", ds.id, conf.Options.Id)

		for {
			resp, _ := redis.MustDecodeOpt(decoder)

			if scmd, argv, err = redis.ParseArgs(resp); err != nil {
				utils.OpLogRotater.Rotate()
				log.PanicErrorf(err, "jdbSyncer[%v] parse command arguments failed", ds.id)
			}

			if strings.EqualFold(scmd, "publish") && strings.EqualFold(string(argv[0]), "__sentinel__:hello") {
				continue
			}
			if strings.EqualFold(scmd, "ping") {
				continue
			}

			ds.sendBuf <- cmdDetail{Cmd: scmd, Args: argv}
		}
	}()

	go func() {

		for item := range ds.sendBuf {
			cmd_args := make([]string, len(item.Args)+3)
			cmd_args[0] = time.Now().Format("2006/01/02-15:04:05")
			cmd_args[1] = item.Cmd

			length := len(item.Cmd)
			for i, ele := range item.Args {
				cmd_args[i+2] = *(*string)(unsafe.Pointer(&ele))
				length += len(ele)
			}
			cmd_args[len(item.Args)+2] = splitFlag
			cmdArgsLine := strings.Join(cmd_args, splitFlag)
			log.OpPurePrintf(cmdArgsLine)

			///* 如下是模拟一个aof的实现
			//firstly 计算 一共有一个部分 比如 SET key-with-expire-time "hello" EX 1008612 为 *5
			//*/
			//lenCmdArgs := len(cmd_args[1:len(cmd_args)-1]) // cmd_args[0]是时间 不要先
			///* secondly 计算要写入的长度 参考redis aof的写法 遍历args */
			//rediscommand := fmt.Sprintf("%v\r\n*%v\r\n", time.Now().Format("2006/01/02-15:04:05"), lenCmdArgs)
			//for _, str := range cmd_args[1:len(cmd_args)-1] {
			//	strlength := len([]rune(str))
			//	rediscommand = rediscommand + "$" + strconv.Itoa(strlength) + "\r\n" + str +"\r\n"
			//}
			//fmt.Println(rediscommand)

			ds.forward.Incr()
			ds.wbytes.Add(int64(length))
			metric.GetMetric(ds.id).AddPushCmdCount(ds.id, 1)
			metric.GetMetric(ds.id).AddNetworkFlow(ds.id, uint64(length))
			sendId.Incr()

		}
	}()

	for lstat := ds.Stat(); ; {
		time.Sleep(time.Second)
		nstat := ds.Stat()
		var b bytes.Buffer
		_, _ = fmt.Fprintf(&b, "jdbSyncer[%v] sync: ", ds.id)
		_, _ = fmt.Fprintf(&b, " +forwardCommands=%-6d", nstat.forward-lstat.forward)
		_, _ = fmt.Fprintf(&b, " +filterCommands=%-6d", nstat.nbypass-lstat.nbypass)
		_, _ = fmt.Fprintf(&b, " +writeBytes=%d", nstat.wbytes-lstat.wbytes)
		log.Debug(b.String())
		lstat = nstat
	}
}
