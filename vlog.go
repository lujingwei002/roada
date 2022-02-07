package roada

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type vlogFile struct {
	path   string
	minVer int64
	maxVer int64
}

//读取并返回大于version的记录
func (self *vlogFile) readLog(version int64) ([]string, error) {
	file, err := os.Open(self.path)
	if nil != err {
		return nil, err
	}
	defer file.Close()
	buff := bufio.NewReader(file)
	recordArr := make([]string, 0)
	for {
		record, err := buff.ReadString('\n')
		if err == io.EOF {
			return recordArr, nil
		}
		record = strings.TrimSpace(record)
		args := strings.Split(record, "|")
		if len(args) < 2 {
			continue
		}
		if ver, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			continue
		} else if ver <= version {
			continue
		} else {
			recordArr = append(recordArr, record)
		}
	}
	return recordArr, nil
}

type vlogFileSlice []*vlogFile

func (self vlogFileSlice) Len() int {
	return len(self)
}

func (self vlogFileSlice) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self vlogFileSlice) Less(i, j int) bool {
	return self[i].maxVer < self[j].maxVer
}

func newVlogFile(path string) *vlogFile {
	name := filepath.Base(path)
	if filepath.Ext(name) != ".log" {
		return nil
	}
	args := strings.Split(strings.Replace(name, filepath.Ext(name), "", 1), "_")
	if len(args) != 4 {
		return nil
	}
	minVer, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil
	}
	maxVer, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return nil
	}
	return &vlogFile{
		path:   path,
		minVer: minVer,
		maxVer: maxVer,
	}
}

type vlog struct {
	file     *os.File
	dataDir  string
	road     *Road
	nameVer  int64
	logVer   int64 //当前日志文件的起始版本-1
	cacheVer int64 //cache里第一个元素的版本
	cache    []string
}

func newVlog(road *Road) *vlog {
	self := &vlog{
		road:     road,
		cacheVer: 0,
		cache:    make([]string, 0),
	}
	return self
}

func (self *vlog) Create(nameVer int64) error {
	self.nameVer = nameVer
	self.dataDir = fmt.Sprintf("./coord#%d", nameVer)
	//创建目录
	if _, err := os.Stat(self.dataDir); err != nil && os.IsNotExist(err) {
		if err := os.Mkdir(self.dataDir, 0666); err != nil {
			return err
		}
	}
	//创建日志文件
	if err := self.openLogFile(); err != nil {
		return err
	}
	return nil
}

//恢复recordArr
func (self *vlog) RestoreCache(slaveVer int64, version int64) error {
	t1 := time.Now()
	if _, err := os.Stat(self.dataDir); err != nil && os.IsNotExist(err) {
		return err
	}
	files, err := ioutil.ReadDir(self.dataDir)
	if err != nil {
		return err
	}
	logFileArr := make(vlogFileSlice, 0)
	for _, file := range files {
		if file.IsDir() {
			continue
		} else {
			logFile := newVlogFile(fmt.Sprintf("%s/%s", self.dataDir, file.Name()))
			if logFile == nil {
				continue
			}
			if logFile.maxVer <= slaveVer {
				continue
			}
			logFileArr = append(logFileArr, logFile)
		}
	}
	sort.Sort(logFileArr)
	recordArr := make([]string, 0)
	for _, logFile := range logFileArr {
		lineArr, err := logFile.readLog(slaveVer)
		if err != nil {
			continue
		}
		recordArr = append(recordArr, lineArr...)
	}
	self.cache = recordArr
	if len(self.cache) > 0 {
		args := strings.Split(self.cache[0], "|")
		if ver, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return err
		} else {
			self.cacheVer = ver
		}
	} else {
		self.cacheVer = 0
	}
	log.Printf("[vlog] restoreCache, slaveVer=%d, version=%d, cacheVer=%d-%d, cost=%+v\n",
		slaveVer, version, self.cacheVer,
		If(self.cacheVer+int64(len(self.cache))-1 > 0, self.cacheVer+int64(len(self.cache))-1, 0), time.Since(t1))
	if int64(len(self.cache)) != version-slaveVer {
		return fmt.Errorf("[vlog] restoreCache failed, exptect len(cache)=%d", version-slaveVer)
	}
	return nil
}

func (self *vlog) Pull(version int64) []string {
	if self.cacheVer <= 0 {
		return []string{}
	}
	index := version + 1 - self.cacheVer
	if index < 0 {
		return []string{}
	}
	if index >= int64(len(self.cache)) {
		return []string{}
	}
	return self.cache[index:]
}

func (self *vlog) NodeAdd(version int64, nodeName string, nodeFullName string, nodeAddr string) error {
	logTime := time.Now().Unix()
	str := fmt.Sprintf("node_add|%d|%d|%s|%s|%s", version, logTime, nodeName, nodeFullName, nodeAddr)
	return self.record(version, str)
}

func (self *vlog) NodeDel(version int64, nodeFullName string, reason int) error {
	logTime := time.Now().Unix()
	str := fmt.Sprintf("node_del|%d|%d|%s|%d", version, logTime, nodeFullName, reason)
	return self.record(version, str)
}

func (self *vlog) NodeDead(version int64, nodeFullName string) error {
	logTime := time.Now().Unix()
	str := fmt.Sprintf("node_dead|%d|%d|%s", version, logTime, nodeFullName)
	return self.record(version, str)
}

func (self *vlog) Set(version int64, rkey string, nodeFullName string) error {
	logTime := time.Now().Unix()
	str := fmt.Sprintf("set|%d|%d|%s|%s", version, logTime, rkey, nodeFullName)
	return self.record(version, str)
}

func (self *vlog) Del(version int64, rkey string, nodeFullName string) error {
	logTime := time.Now().Unix()
	str := fmt.Sprintf("del|%d|%d|%s|%s", version, logTime, rkey, nodeFullName)
	return self.record(version, str)
}

func (self *vlog) GroupAdd(version int64, rkey string, member string, nodeFullName string) error {
	logTime := time.Now().Unix()
	str := fmt.Sprintf("group_add|%d|%d|%s|%s|%s", version, logTime, rkey, member, nodeFullName)
	return self.record(version, str)
}

func (self *vlog) GroupDel(version int64, rkey string, member string, nodeFullName string) error {
	logTime := time.Now().Unix()
	str := fmt.Sprintf("group_del|%d|%d|%s|%s|%s", version, logTime, rkey, member, nodeFullName)
	return self.record(version, str)
}

func (self *vlog) Discard(slaveVer int64, version int64) error {
	log.Printf("[vlog] discard, slaveVer=%d, version=%d, cacheVer=%d-%d\n",
		slaveVer, version, self.cacheVer, If(self.cacheVer+int64(len(self.cache))-1 > 0, self.cacheVer+int64(len(self.cache))-1, 0))
	if self.cacheVer <= 0 {
		return nil
	}
	if len(self.cache) <= 0 {
		return nil
	}
	discardLen := slaveVer - self.cacheVer + 1
	if discardLen < 0 {
		return nil
	}
	if discardLen > int64(len(self.cache)) {
		return nil
	}
	self.cache = self.cache[discardLen:]
	if len(self.cache) > 0 {
		args := strings.Split(self.cache[0], "|")
		if ver, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return err
		} else {
			self.cacheVer = ver
		}
	} else {
		self.cacheVer = 0
	}
	log.Printf("[vlog] discard, slaveVer=%d, version=%d, cacheVer=%d-%d, discardLen=%d\n",
		slaveVer, version, self.cacheVer, If(self.cacheVer+int64(len(self.cache))-1 > 0, self.cacheVer+int64(len(self.cache))-1, 0), discardLen)
	return nil
}

func (self *vlog) record(version int64, str string) error {
	log.Printf("[registry] %s\n", str)
	self.cache = append(self.cache, str)
	if self.cacheVer == 0 {
		self.cacheVer = version
	}
	if self.logVer == 0 {
		self.logVer = version
	}
	self.file.WriteString(str + "\n")
	self.checkRotateFile(version)
	return nil
}

func (self *vlog) checkRotateFile(version int64) error {
	lineCount := version - self.logVer + 1
	if lineCount <= 10000 {
		return nil
	}
	if err := self.rotateFile(version); err != nil {
		return err
	}
	if self.file != nil {
		self.file.Close()
		self.file = nil
	}
	if err := self.openLogFile(); err != nil {
		return err
	}
	self.logVer = 0
	return nil
}

func (self *vlog) rotateFile(version int64) error {
	currentFilePath := fmt.Sprintf("%s/coord#%d_vlog.log", self.dataDir, self.nameVer)
	backupFilePath := fmt.Sprintf("%s/coord#%d_vlog_%d_%d.log",
		self.dataDir, self.nameVer, self.logVer, version)
	if _, err := os.Stat(backupFilePath); err == nil {
		return nil
	}
	if err := os.Rename(currentFilePath, backupFilePath); err != nil {
		return err
	}
	return nil
}

func (self *vlog) openLogFile() error {
	filePath := fmt.Sprintf("%s/coord#%d_vlog.log", self.dataDir, self.nameVer)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("[vlog] openLogFile failed, error=%s\n", err.Error())
		return nil
	}
	self.file = file
	self.logVer = 0
	return nil
}

//启动时，规范化coord#%d_vlog.log文件
func (registry *Registry) renameLogFile(dataDir string, nameVer int64) error {
	currentFilePath := fmt.Sprintf("%s/coord#%d_vlog.log", dataDir, nameVer)
	if _, err := os.Stat(currentFilePath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	//search start version and end version
	file, err := os.Open(currentFilePath)
	if nil != err {
		return err
	}
	defer file.Close()
	var startLogVer int64 = -1
	var lastLogVer int64 = -1
	buff := bufio.NewReader(file)
	var lastLine string = ""
	for {
		line, err := buff.ReadString('\n')
		if err == io.EOF {
			if lastLine == "" {
				break
			}
			lastLine = strings.TrimSpace(lastLine)
			args := strings.Split(lastLine, "|")
			if len(args) < 2 {
				return fmt.Errorf("vlog file format error1")
			}
			if ver, err := strconv.ParseInt(args[1], 10, 64); err != nil {
				return err
			} else {
				lastLogVer = ver
			}
		}
		lastLine = line
		if startLogVer == -1 {
			line = strings.TrimSpace(line)
			args := strings.Split(line, "|")
			if len(args) < 2 {
				return fmt.Errorf("vlog file format error2")
			}
			if ver, err := strconv.ParseInt(args[1], 10, 64); err != nil {
				return err
			} else {
				startLogVer = ver
			}
		}
	}
	if startLogVer != -1 && lastLogVer != -1 {
		backupFilePath := fmt.Sprintf("%s/coord#%d_vlog_%d_%d.log",
			dataDir, nameVer, startLogVer, lastLogVer)
		if _, err := os.Stat(backupFilePath); err == nil {
			return fmt.Errorf("vlog file:%s exists", backupFilePath)
		}
		if err := os.Rename(currentFilePath, backupFilePath); err != nil {
			return err
		}
	}
	return nil
}
