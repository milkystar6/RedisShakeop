package run

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/alibaba/RedisShake/pkg/libs/atomic2"
	"github.com/alibaba/RedisShake/pkg/libs/log"
	"github.com/alibaba/RedisShake/pkg/redis"
	"github.com/alibaba/RedisShake/redis-shake/base"
	utils "github.com/alibaba/RedisShake/redis-shake/common"
	conf "github.com/alibaba/RedisShake/redis-shake/configure"
	"github.com/alibaba/RedisShake/redis-shake/filter"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CmdJRestore struct {
}
type cmdInfo struct {
	Cmd  string
	Args []string
}

func (cmd *CmdJRestore) GetDetailedInfo() interface{} {
	return nil
}
func (cmd *CmdJRestore) Main() {
	if len(conf.Options.SourceRdbInput) > 0 {
		type restoreNode struct {
			id    int
			input string
		}
		base.Status = "waitRestore"
		total := utils.GetTotalLink()
		restoreChan := make(chan restoreNode, total)
		for i, rdb := range conf.Options.SourceRdbInput {
			restoreChan <- restoreNode{id: i, input: rdb}
		}

		var wg sync.WaitGroup
		wg.Add(len(conf.Options.SourceRdbInput))
		for i := 0; i < conf.Options.SourceRdbParallel; i++ {
			go func() {
				for {
					node, ok := <-restoreChan
					if !ok {
						break
					}

					var target []string
					if conf.Options.TargetType == conf.RedisTypeCluster {
						target = conf.Options.TargetAddressList
					} else {
						// round-robin pick
						pick := utils.PickTargetRoundRobin(len(conf.Options.TargetAddressList))
						target = []string{conf.Options.TargetAddressList[pick]}
					}

					dr := &jdbRestorer{
						id:             node.id,
						input:          node.input,
						target:         target,
						targetPassword: conf.Options.TargetPasswordRaw,
					}
					log.Infof("routine[%v] starts restoring data from %v to %v",
						dr.id, dr.input, dr.target)
					dr.restore()

					wg.Done()
				}
			}()
		}

		wg.Wait()
		close(restoreChan)

		log.Infof("restore from '%s' to '%s' done", conf.Options.SourceRdbInput, conf.Options.TargetAddressList)
	}

	if len(conf.Options.SourceOplogInput) > 0 {
		oplogEndtime := conf.Options.OplogEndtime
		if _, err := time.Parse("2006/01/02-15:04:05", oplogEndtime); err != nil {
			log.Panicf("Event:parse oplogendtime\tError:%s\t", err.Error())
		}

		readeTimeout := time.Duration(10) * time.Minute
		writeTimeout := time.Duration(10) * time.Minute
		isCluster := conf.Options.TargetType == conf.RedisTypeCluster
		var target []string
		if conf.Options.TargetType == conf.RedisTypeCluster {
			target = conf.Options.TargetAddressList
		} else {
			// round-robin pick
			pick := utils.PickTargetRoundRobin(len(conf.Options.TargetAddressList))
			target = []string{conf.Options.TargetAddressList[pick]}
		}
		c := utils.OpenRedisConnWithTimeout(target, conf.Options.TargetAuthType, conf.Options.TargetPasswordRaw, readeTimeout, writeTimeout, isCluster, conf.Options.TargetTLSEnable, conf.Options.TargetTLSSkipVerify)
		defer c.Close()

		sendBuf := make(chan cmdInfo, conf.Options.SenderCount)

		go func() {
			for _, fileName := range conf.Options.SourceOplogInput {
				readin, _ := utils.OpenReadFile(fileName)
				defer readin.Close()
				reader := bufio.NewReaderSize(readin, utils.ReaderBufferSize)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						if err == io.EOF {
							break
						}
						log.Panicf("Event:ReadLineFail\tError:%s\t", err.Error())
					}
					line = line[:len(line)-1]
					ele := strings.SplitN(line, splitFlag, 3)
					Argv := strings.Split(ele[2], splitFlag)
					Argv = Argv[:len(Argv)-1]
					if ele[0] <= oplogEndtime {
						sendBuf <- cmdInfo{Cmd: ele[1], Args: Argv}
					} else {
						break
					}

				}
			}
			close(sendBuf)
		}()

		count := 0
		for item := range sendBuf {
			data := make([]interface{}, len(item.Args))
			for i := range item.Args {
				data[i] = item.Args[i]
			}
			err := c.Send(item.Cmd, data...)
			if err != nil {
				log.Panicf("Event:SendToTargetFail\tError:%s\t", err.Error())
			}
			count++
			if count == 100 {
				utils.FlushAndCheckReply(c, count)
				count = 0
			}
		}
		utils.FlushAndCheckReply(c, count)

		log.Infof("restore oplog '%s' to '%s' done ", conf.Options.SourceOplogInput, conf.Options.TargetAddressList)

	}

}

/*------------------------------------------------------*/
// one restore link corresponding to one jdbRestorer
type jdbRestorer struct {
	id             int      // id
	input          string   // input rdb
	target         []string // len >= 1 when target type is cluster, otherwise len == 1
	targetPassword string

	// metric
	rbytes, ebytes, nentry, ignore atomic2.Int64
	forward, nbypass               atomic2.Int64
}

func (dr *jdbRestorer) Stat() *cmdRestoreStat {
	return &cmdRestoreStat{
		rbytes: dr.rbytes.Get(),
		ebytes: dr.ebytes.Get(),
		nentry: dr.nentry.Get(),
		ignore: dr.ignore.Get(),

		forward: dr.forward.Get(),
		nbypass: dr.nbypass.Get(),
	}
}

func (dr *jdbRestorer) restore() {
	readin, nsize := utils.OpenReadFile(dr.input)
	defer readin.Close()
	base.Status = "restore"

	reader := bufio.NewReaderSize(readin, utils.ReaderBufferSize)

	dr.restoreRDBFile(reader, dr.target, conf.Options.TargetAuthType, conf.Options.TargetPasswordRaw,
		nsize, conf.Options.TargetTLSEnable, conf.Options.TargetTLSSkipVerify)

	base.Status = "extra"
	if conf.Options.ExtraInfo && (nsize == 0 || nsize != dr.rbytes.Get()) {
		// inner usage
		dr.restoreCommand(reader, dr.target, conf.Options.TargetAuthType,
			conf.Options.TargetPasswordRaw, conf.Options.TargetTLSEnable, conf.Options.TargetTLSSkipVerify)
	}
}

func (dr *jdbRestorer) restoreRDBFile(reader *bufio.Reader, target []string, auth_type, passwd string, nsize int64,
	tlsEnable bool, tlsSkipVerify bool) {
	pipe := utils.NewRDBLoader(reader, &dr.rbytes, base.RDBPipeSize)
	wait := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(conf.Options.Parallel)
		for i := 0; i < conf.Options.Parallel; i++ {
			go func() {
				defer wg.Done()
				c := utils.OpenRedisConn(target, auth_type, passwd, conf.Options.TargetType == conf.RedisTypeCluster,
					tlsEnable, tlsSkipVerify)
				defer c.Close()
				var lastdb uint32 = 0
				for e := range pipe {
					if filter.FilterDB(int(e.DB)) {
						// filter db
						dr.ignore.Incr()
					} else {
						dr.nentry.Incr()

						log.Debugf("routine[%v] try restore key[%s] with value length[%v]", dr.id, e.Key, len(e.Value))

						if conf.Options.TargetDB != -1 {
							if conf.Options.TargetDB != int(lastdb) {
								lastdb = uint32(conf.Options.TargetDB)
								utils.SelectDB(c, lastdb)
							}
						} else {
							if e.DB != lastdb {
								lastdb = e.DB
								utils.SelectDB(c, lastdb)
							}
						}

						if filter.FilterKey(string(e.Key)) {
							continue
						}

						log.Debugf("routine[%v] start restoring key[%s] with value length[%v]", dr.id, e.Key, len(e.Value))

						utils.RestoreRdbEntry(c, e)
						log.Debugf("routine[%v] restore key[%s] ok", dr.id, e.Key)
					}
				}
			}()
		}
		wg.Wait()
		close(wait)
	}()

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		stat := dr.Stat()
		var b bytes.Buffer
		if nsize != 0 {
			fmt.Fprintf(&b, "routine[%v] total = %s - %12s [%3d%%]", dr.id, utils.GetMetric(nsize),
				utils.GetMetric(stat.rbytes), 100*stat.rbytes/nsize)
		} else {
			fmt.Fprintf(&b, "routine[%v] total = %12s", dr.id, utils.GetMetric(stat.rbytes))
		}
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		if stat.ignore != 0 {
			fmt.Fprintf(&b, "  ignore=%-12d", stat.ignore)
		}
		log.Info(b.String())
	}
	log.Infof("routine[%v] restore: rdb done", dr.id)
}

func (dr *jdbRestorer) restoreCommand(reader *bufio.Reader, target []string, auth_type, passwd string, tlsEnable bool, tlsSkipVerify bool) {
	// inner usage. only use on targe
	c := utils.OpenNetConn(target[0], auth_type, passwd, tlsEnable, tlsSkipVerify)
	defer c.Close()

	writer := bufio.NewWriterSize(c, utils.WriterBufferSize)
	defer utils.FlushWriter(writer)

	// discard target returning
	go func() {
		p := make([]byte, utils.ReaderBufferSize)
		for {
			utils.Iocopy(c, ioutil.Discard, p, len(p))
		}
	}()

	go func() {
		var bypass = false
		for {
			resp := redis.MustDecode(reader)
			if scmd, args, err := redis.ParseArgs(resp); err != nil {
				log.PanicError(err, "routine[%v] parse command arguments failed", dr.id)
			} else if scmd != "ping" {
				if scmd == "select" {
					if len(args) != 1 {
						log.Panicf("routine[%v] select command len(args) = %d", dr.id, len(args))
					}
					s := string(args[0])
					n, err := strconv.Atoi(s)
					if err != nil {
						log.PanicErrorf(err, "routine[%v] parse db = %s failed", dr.id, s)
					}
					bypass = filter.FilterDB(n)
				}
				if bypass {
					dr.nbypass.Incr()
					continue
				}
			}
			dr.forward.Incr()
			redis.MustEncode(writer, resp)
			utils.FlushWriter(writer)
		}
	}()

	for lstat := dr.Stat(); ; {
		time.Sleep(time.Second)
		nstat := dr.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "routine[%v] restore: ", dr.id)
		fmt.Fprintf(&b, " +forward=%-6d", nstat.forward-lstat.forward)
		fmt.Fprintf(&b, " +nbypass=%-6d", nstat.nbypass-lstat.nbypass)
		log.Info(b.String())
		lstat = nstat
	}
}
