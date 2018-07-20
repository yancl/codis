// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/docopt/docopt-go"

	"bytes"
	"encoding/json"
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/topom"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"io/ioutil"
	"net/http"
	"strings"
)

func main() {
	const usage = `
Usage:
	codis-ha [--log=FILE] [--log-level=LEVEL] [--interval=SECONDS] --dashboard=ADDR [--no-maintains] --slack-url=ADDR --slack-channel=CHANNEL --slack-username=USERNAME --slack-notify-usernames=NOTIFYNAMES
	codis-ha  --version

Options:
	-l FILE, --log=FILE         set path/name of daliy rotated log file.
	--log-level=LEVEL           set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
`
	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	if d["--version"].(bool) {
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return
	}

	if s, ok := utils.Argument(d, "--log"); ok {
		w, err := log.NewRollingFile(s, log.DailyRolling)
		if err != nil {
			log.PanicErrorf(err, "open log file %s failed", s)
		} else {
			log.StdLog = log.New(w, "")
		}
	}
	log.SetLevel(log.LevelInfo)

	if s, ok := utils.Argument(d, "--log-level"); ok {
		if !log.SetLevelString(s) {
			log.Panicf("option --log-level = %s", s)
		}
	}

	var interval = 5
	if n, ok := utils.ArgumentInteger(d, "--interval"); ok {
		if n <= 0 {
			log.Panicf("option --interval = %d", n)
		}
		interval = n
	}

	dashboard := utils.ArgumentMust(d, "--dashboard")
	log.Warnf("set dashboard = %s", dashboard)
	log.Warnf("set interval = %d (seconds)", interval)

	var maintains = true
	if d["--no-maintains"].(bool) {
		maintains = false
	}

	// parse slack info
	slackInfo := SlackInfo{}
	slackInfo.url = utils.ArgumentMust(d, "--slack-url")
	slackInfo.channel = utils.ArgumentMust(d, "--slack-channel")
	slackInfo.userName = utils.ArgumentMust(d, "--slack-username")
	slackInfo.notifyUserNames = strings.Split(utils.ArgumentMust(d, "--slack-notify-usernames"), ",")

	badSlaves = make(map[string]int, 1000)
	badProxies = make(map[string]int, 10)

	client := topom.NewApiClient(dashboard)

	t, err := client.Model()
	if err != nil {
		log.PanicErrorf(err, "rpc fetch model failed")
	}
	log.Warnf("topom =\n%s", t.Encode())

	client.SetXAuth(t.ProductName)

	overview, err := client.Overview()
	if err != nil {
		log.PanicErrorf(err, "rpc fetch overview failed")
	}
	prodcutAuth := overview.Config.ProductAuth

	for {
		hc := newHealthyChecker(client)
		hc.slackInfo = &slackInfo
		hc.LogProxyStats()
		hc.LogGroupStats()
		if maintains {
			hc.Maintains(client, interval*10, prodcutAuth)
		}

		// remove proxies&slaves offlined manually
		hc.RemoveOfflinedProxies()
		hc.RemoveOfflinedSlaves()

		time.Sleep(time.Second * time.Duration(interval))
	}
}

type Status int

const (
	OK Status = iota
	ERROR
)

const (
	IconEmoji      string = ":robot_face:"
	HealthyEmoji   string = ":good:"
	UnhealthyEmoji string = ":unhealthy:"
)

type SlackInfo struct {
	url             string
	channel         string
	userName        string
	notifyUserNames []string
}

type SlackPayload struct {
	Channel   string `json:"channel"`
	UserName  string `json:"username"`
	Text      string `json:"text"`
	IconEmoji string `json:"icon_emoji"`
}

var badSlaves map[string]int
var badProxies map[string]int

const (
	CodeAlive = iota + 100
	CodeError
	CodeMissing
	CodeTimeout
)

const (
	CodeSyncReady = iota + 200
	CodeSyncError
	CodeSyncBroken
)

var codeMapping = map[int]string{
	CodeAlive:      "CodeAlive",
	CodeError:      "CodeError",
	CodeMissing:    "CodeMissing",
	CodeTimeout:    "CodeTimeout",
	CodeSyncReady:  "CodeSyncReady",
	CodeSyncError:  "CodeSyncError",
	CodeSyncBroken: "CodeSyncBroken",
}

type HealthyChecker struct {
	slackInfo *SlackInfo
	*topom.Stats
	pstatus map[string]int
	sstatus map[string]int
}

func newHealthyChecker(client *topom.ApiClient) *HealthyChecker {
	stats, err := client.Stats()
	if err != nil {
		log.PanicErrorf(err, "rpc stats failed")
	}

	hc := &HealthyChecker{Stats: stats}

	hc.pstatus = make(map[string]int)
	for _, p := range hc.Proxy.Models {
		switch stats := hc.Proxy.Stats[p.Token]; {
		case stats == nil:
			hc.pstatus[p.Token] = CodeMissing
		case stats.Error != nil:
			hc.pstatus[p.Token] = CodeError
		case stats.Timeout || stats.Stats == nil:
			hc.pstatus[p.Token] = CodeTimeout
		default:
			hc.pstatus[p.Token] = CodeAlive
		}
	}

	hc.sstatus = make(map[string]int)
	for _, g := range hc.Group.Models {
		for i, x := range g.Servers {
			var addr = x.Addr
			switch stats := hc.Group.Stats[addr]; {
			case stats == nil:
				hc.sstatus[addr] = CodeMissing
			case stats.Error != nil:
				hc.sstatus[addr] = CodeError
			case stats.Timeout || stats.Stats == nil:
				hc.sstatus[addr] = CodeTimeout
			default:
				if i == 0 {
					if stats.Stats["master_addr"] != "" {
						hc.sstatus[addr] = CodeSyncError
					} else {
						hc.sstatus[addr] = CodeSyncReady
					}
				} else {
					if stats.Stats["master_addr"] != g.Servers[0].Addr {
						hc.sstatus[addr] = CodeSyncError
					} else {
						switch stats.Stats["master_link_status"] {
						default:
							hc.sstatus[addr] = CodeSyncError
						case "up":
							hc.sstatus[addr] = CodeSyncReady
						case "down":
							hc.sstatus[addr] = CodeSyncBroken
						}
					}
				}
			}
		}
	}
	return hc
}

func (hc *HealthyChecker) LogProxyStats() {
	var format string
	var wpid int
	for _, p := range hc.Proxy.Models {
		wpid = math2.MaxInt(wpid, len(strconv.Itoa(p.Id)))
	}
	format += fmt.Sprintf("proxy-%%0%dd [T] %%s", wpid)

	var waddr1, waddr2 int
	for _, p := range hc.Proxy.Models {
		waddr1 = math2.MaxInt(waddr1, len(p.AdminAddr))
		waddr2 = math2.MaxInt(waddr2, len(p.ProxyAddr))
	}
	format += fmt.Sprintf(" [A] %%-%ds", waddr1)
	format += fmt.Sprintf(" [P] %%-%ds", waddr2)

	for _, p := range hc.Proxy.Models {
		switch hc.pstatus[p.Token] {
		case CodeMissing:
			log.Warnf("[?] "+format, p.Id, p.Token, p.AdminAddr, p.ProxyAddr)
		case CodeError:
			log.Warnf("[E] "+format, p.Id, p.Token, p.AdminAddr, p.ProxyAddr)
		case CodeTimeout:
			log.Warnf("[T] "+format, p.Id, p.Token, p.AdminAddr, p.ProxyAddr)
		default:
			log.Infof("[ ] "+format, p.Id, p.Token, p.AdminAddr, p.ProxyAddr)
		}
	}
}

func (hc *HealthyChecker) LogGroupStats() {
	var format string
	var wgid, widx int
	for _, g := range hc.Group.Models {
		wgid = math2.MaxInt(wgid, len(strconv.Itoa(g.Id)))
		for i, _ := range g.Servers {
			widx = math2.MaxInt(widx, len(strconv.Itoa(i)))
		}
	}
	format += fmt.Sprintf("group-%%0%dd [%%0%dd]", wgid, widx)

	var waddr int
	for _, g := range hc.Group.Models {
		for _, x := range g.Servers {
			waddr = math2.MaxInt(waddr, len(x.Addr))
		}
	}
	format += fmt.Sprintf(" %%-%ds", waddr)

	for _, g := range hc.Group.Models {
		for i, x := range g.Servers {
			switch hc.sstatus[x.Addr] {
			case CodeMissing:
				log.Warnf("[?] "+format, g.Id, i, x.Addr)
			case CodeError:
				log.Warnf("[E] "+format, g.Id, i, x.Addr)
			case CodeTimeout:
				log.Warnf("[T] "+format, g.Id, i, x.Addr)
			case CodeSyncReady:
				log.Infof("[ ] "+format, g.Id, i, x.Addr)
			case CodeSyncError, CodeSyncBroken:
				log.Warnf("[X] "+format, g.Id, i, x.Addr)
			}
		}
	}
}

func (hc *HealthyChecker) Maintains(client *topom.ApiClient, maxdown int, auth string) {
	// remove proxy at state error from codis
	for _, p := range hc.Proxy.Models {
		switch hc.pstatus[p.Token] {
		case CodeError, CodeTimeout, CodeMissing:
			/*
				log.Warnf("try to remove proxy-[%s]", p.AdminAddr)
				if err := client.RemoveProxy(p.Token, true); err != nil {
					log.ErrorErrorf(err, "call rpc remove-proxy to dashboard %s failed", p.AdminAddr)
					return
				}
				log.Warnf("try to remove proxy done.")
				return
			*/
			hc.AddBadProxy(p, hc.pstatus)
		default:
			hc.RecoverProxy(p, hc.pstatus)
			continue
		}
	}

	// remove server at state error from codis
Groups:
	for _, g := range hc.Group.Models {
		for i, x := range g.Servers {
			// if master state is not right, promote slave to master first(if have slave)
			if i == 0 {
				if len(g.Servers) > 1 {
					switch hc.sstatus[g.Servers[0].Addr] {
					case CodeError, CodeMissing, CodeTimeout, CodeSyncError:
						log.Warnf("codis-server (master) %s state error", x.Addr)
						sendSlackMessage(hc.slackInfo, ERROR,
							fmt.Sprintf("codis server(master) is broken, group:%d, addr:%s, code:%s", g.Id, g.Servers[0].Addr, codeMapping[hc.sstatus[g.Servers[0].Addr]]))
						break Groups
					default:
						continue
					}
				} else {
					continue
				}
			}
			// remove codis server(slave and only one master) which state is not right
			switch hc.sstatus[x.Addr] {
			/*
				case CodeError, CodeMissing, CodeTimeout, CodeSyncError:
					log.Warnf("try to group-del-server to dashboard %s", x.Addr)
					if err := client.GroupDelServer(g.Id, x.Addr); err != nil {
						log.ErrorErrorf(err, "call rpc group-del-server to dashboard %s failed", x.Addr)
						return
					}
					log.Debugf("call rpc group-del-server OK")

					// trt to shutdown codis-server as slave in error state
					log.Warnf("try to shutdown codis-server(slave) %s", x.Addr)
					c, err := redis.NewClient(x.Addr, auth, time.Minute*30)
					if err != nil {
						log.WarnErrorf(err, "connect to codis-server(slave) %s failed", x.Addr)
						return
					}
					defer c.Close()
					if err := c.Shutdown(); err != nil {
						log.WarnErrorf(err, "try to shutdown codis-server %s failed", x.Addr)
						return
					}
					return
				case CodeSyncBroken:
					log.Warnf("slave %s master link down", x.Addr)
			*/
			case CodeError, CodeMissing, CodeTimeout, CodeSyncError, CodeSyncBroken:
				hc.AddBadSlave(x, hc.sstatus)
			default:
				hc.RecoverBadSlave(x, hc.sstatus)
				continue
			}
		}
	}

	// promote group server
	for _, g := range hc.Group.Models {
		if len(g.Servers) != 0 {
			switch hc.sstatus[g.Servers[0].Addr] {
			case CodeMissing, CodeError, CodeTimeout:
				sendSlackMessage(hc.slackInfo, ERROR,
					fmt.Sprintf("codis server(master) is broken, prepare to failover, group:%d, addr:%s, code:%s",
						g.Id, g.Servers[0].Addr, codeMapping[hc.sstatus[g.Servers[0].Addr]]))
				var synced int
				var picked, picked2 = 0, 0
				var mindown, mindown2 = maxdown + 1, 65535
				for i := 1; i < len(g.Servers); i++ {
					var addr = g.Servers[i].Addr
					switch hc.sstatus[addr] {
					case CodeSyncReady:
						synced++
					case CodeSyncBroken:
						if stats := hc.Group.Stats[addr]; stats != nil && stats.Stats != nil {
							n, err := strconv.Atoi(stats.Stats["master_link_down_since_seconds"])
							if err != nil {
								log.WarnErrorf(err, "try to get %s master_link_down_since_seconds failed", addr)
								continue
							}
							if n >= 0 && n < mindown {
								picked, mindown = i, n
							}
							if picked == 0 && n >= 0 && n < mindown2 {
								picked2, mindown2 = i, n
							}
						}
					case CodeSyncError:
						if picked == 0 && picked2 == 0 {
							picked2 = i
						}
					}
				}
				switch {
				case picked == 0 && picked2 == 0:
					log.Warnf("try to promote group-[%d], but synced = %d & picked = %d & picked2 = %d, giveup", g.Id, synced, picked, picked2)
					sendSlackMessage(hc.slackInfo, ERROR,
						fmt.Sprintf("codis server(master) failover failed, no good slave available"))
					//fmt.Sprintf("codis server(master) failover failed, no good slave available, group:%d,addr:%s", g.Id, g.Servers[0].Addr))
				case g.Promoting.State != "":
					log.Warnf("try to promote group-[%d], but group is promoting = %s, please fix it manually", g.Id, g.Promoting.State)
					sendSlackMessage(hc.slackInfo, ERROR,
						fmt.Sprintf("codis server(master) failover failed, group id promoting, group:%d,addr:%s", g.Id, g.Servers[0].Addr))
				case picked > 0 || picked2 > 0:
					var pick int
					if picked > 0 {
						pick = picked
					} else {
						pick = picked2
					}
					var slave = g.Servers[pick].Addr
					log.Warnf("try to promote group-[%d] with slave %s", g.Id, slave)
					if err := client.GroupPromoteServer(g.Id, slave); err != nil {
						log.ErrorErrorf(err, "rpc promote server failed")
					}
					log.Warnf("done.")
					sendSlackMessage(hc.slackInfo, OK,
						fmt.Sprintf("codis server(master) failover done, group:%d, addr:%s-->%s", g.Id, g.Servers[0].Addr, slave))
				}
			}
		}
	}
}

func sendSlackMessage(slackInfo *SlackInfo, status Status, message string) {
	// build request
	payload := SlackPayload{
		Channel:   slackInfo.channel,
		UserName:  slackInfo.userName,
		IconEmoji: IconEmoji,
	}

	emoji := UnhealthyEmoji
	if status == OK {
		emoji = HealthyEmoji
	}

	notifyUsers := make([]string, 0, len(slackInfo.notifyUserNames))
	for _, userName := range slackInfo.notifyUserNames {
		notifyUsers = append(notifyUsers, fmt.Sprintf("<@%s>", userName))
	}

	payload.Text = fmt.Sprintf("%s %s %s", emoji, message, strings.Join(notifyUsers, ","))

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Warnf("json marshal failed, err:%s", err)
		return
	}

	// send request
	req, err := http.NewRequest("POST", slackInfo.url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Warnf("new http request failed, err:%s", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	rsp, err := client.Do(req)
	if err != nil {
		log.Warnf("do http request failed, err:%s", err)
		return
	}
	defer rsp.Body.Close()

	// process response
	log.Infof("response status code:%s", rsp.Status)
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		log.Warnf("read http response body failed, err:%s", err)
		return
	}
	log.Infof("response body:%s", string(body))
}

func (hc *HealthyChecker) AddBadProxy(p *models.Proxy, pstatus map[string]int) {
	if _, exit := badProxies[p.ProxyAddr]; !exit {
		badProxies[p.ProxyAddr] = 1
		sendSlackMessage(hc.slackInfo, ERROR,
			fmt.Sprintf("proxy is broken, product name:%s, addr:%s, token:%s, code:%s", p.ProductName, p.ProxyAddr, p.Token, codeMapping[pstatus[p.Token]]))
	}
}

func (hc *HealthyChecker) RecoverProxy(p *models.Proxy, pstatus map[string]int) {
	if _, exit := badProxies[p.ProxyAddr]; exit {
		delete(badProxies, p.ProxyAddr)
		sendSlackMessage(hc.slackInfo, OK,
			fmt.Sprintf("proxy is recovered, product name:%s, addr:%s, token:%s", p.ProductName, p.ProxyAddr, p.Token))
	}
}

func (hc *HealthyChecker) RemoveOfflinedProxies() {
	onlineProxies := make(map[string]int, 100)

	for _, p := range hc.Proxy.Models {
		onlineProxies[p.ProxyAddr] = 1
	}

	for addr, _ := range badProxies {
		if _, exit := onlineProxies[addr]; !exit {
			delete(badProxies, addr)
			sendSlackMessage(hc.slackInfo, OK, fmt.Sprintf("proxy is offlined, addr:%s", addr))
		}
	}
}

func (hc *HealthyChecker) AddBadSlave(s *models.GroupServer, sstatus map[string]int) {
	if _, exit := badSlaves[s.Addr]; !exit {
		badSlaves[s.Addr] = 1
		msg := fmt.Sprintf("codis server(slave) is broken, addr:%s, code:%s", s.Addr, codeMapping[sstatus[s.Addr]])
		log.Warnf(msg)
		sendSlackMessage(hc.slackInfo, ERROR, msg)
	}
}

func (hc *HealthyChecker) RecoverBadSlave(s *models.GroupServer, sstatus map[string]int) {
	if _, exit := badSlaves[s.Addr]; exit {
		delete(badSlaves, s.Addr)
		msg := fmt.Sprintf("codis server(slave) is recovered, addr:%s", s.Addr)
		log.Warnf(msg)
		sendSlackMessage(hc.slackInfo, OK, msg)
	}
}

func (hc *HealthyChecker) RemoveOfflinedSlaves() {
	onlineSlaves := make(map[string]int, 1000)

	for _, g := range hc.Group.Models {
		for _, x := range g.Servers {
			onlineSlaves[x.Addr] = 1
		}
	}

	for addr, _ := range badSlaves {
		if _, exit := onlineSlaves[addr]; !exit {
			delete(badSlaves, addr)
			sendSlackMessage(hc.slackInfo, OK, fmt.Sprintf("server(slave) is offlined, addr:%s", addr))
		}
	}
}
