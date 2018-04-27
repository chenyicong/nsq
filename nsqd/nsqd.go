package nsqd

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex

	opts atomic.Value

	dl        *dirlock.DirLock
	isLoading int32
	errValue  atomic.Value
	startTime time.Time

	topicMap map[string]*Topic

	lookupPeers atomic.Value

	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	poolSize int

	notifyChan           chan interface{} // 新增或删除topic和channel时，会写入notifyChan
	optsNotificationChan chan struct{}    // 配置更改通知的channel
	exitChan             chan int
	waitGroup            util.WaitGroupWrapper

	ci *clusterinfo.ClusterInfo
}

func New(opts *Options) *NSQD {
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),
	}
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.swapOpts(opts)
	n.errValue.Store(errStore{})

	var err error
	opts.logLevel, err = lg.ParseLogLevel(opts.LogLevel, opts.Verbose)
	if err != nil {
		n.logf(LOG_FATAL, "%s", err)
		os.Exit(1)
	}

	err = n.dl.Lock()
	if err != nil {
		n.logf(LOG_FATAL, "--data-path=%s in use (possibly by another instance of nsqd)", dataPath)
		os.Exit(1)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		n.logf(LOG_FATAL, "--max-deflate-level must be [1,9]")
		os.Exit(1)
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		n.logf(LOG_FATAL, "--node-id must be [0,1024)")
		os.Exit(1)
	}

	if opts.StatsdPrefix != "" {
		var port string
		_, port, err = net.SplitHostPort(opts.HTTPAddress)
		if err != nil {
			n.logf(LOG_FATAL, "failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
			os.Exit(1)
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		n.logf(LOG_FATAL, "failed to build TLS config - %s", err)
		os.Exit(1)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		n.logf(LOG_FATAL, "cannot require TLS client connections without TLS key and cert")
		os.Exit(1)
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			n.logf(LOG_FATAL, "Invalid percentile: %v", v)
			os.Exit(1)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	return n
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.tcpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.httpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.httpsListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

func (n *NSQD) Main() {
	var httpListener net.Listener
	var httpsListener net.Listener

	ctx := &context{n}

	tcpListener, err := net.Listen("tcp", n.getOpts().TCPAddress)
	if err != nil {
		n.logf(LOG_FATAL, "listen (%s) failed - %s", n.getOpts().TCPAddress, err)
		os.Exit(1)
	}
	n.Lock()
	n.tcpListener = tcpListener
	n.Unlock()
	tcpServer := &tcpServer{ctx: ctx}
	// tcp服务
	n.waitGroup.Wrap(func() {
		protocol.TCPServer(n.tcpListener, tcpServer, n.logf)
	})

	// https服务
	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" {
		httpsListener, err = tls.Listen("tcp", n.getOpts().HTTPSAddress, n.tlsConfig)
		if err != nil {
			n.logf(LOG_FATAL, "listen (%s) failed - %s", n.getOpts().HTTPSAddress, err)
			os.Exit(1)
		}
		n.Lock()
		n.httpsListener = httpsListener
		n.Unlock()
		// https和http都是用的newHTTPServer，因为https只是比http多了一个tls层，下面的逻辑是一样的
		httpsServer := newHTTPServer(ctx, true, true)
		n.waitGroup.Wrap(func() {
			http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf)
		})
	}
	// http服务
	httpListener, err = net.Listen("tcp", n.getOpts().HTTPAddress)
	if err != nil {
		n.logf(LOG_FATAL, "listen (%s) failed - %s", n.getOpts().HTTPAddress, err)
		os.Exit(1)
	}
	n.Lock()
	n.httpListener = httpListener
	n.Unlock()
	httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired)
	n.waitGroup.Wrap(func() {
		http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf)
	})

	// queueScan 扫描的是inflightQueue和deferredQueue这两个优先级队列
	n.waitGroup.Wrap(func() { n.queueScanLoop() })
	// lookupLoop处理与nsqlookupd的通信，将topic和channel信息注册到nsqlookupd等
	n.waitGroup.Wrap(func() { n.lookupLoop() })
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(func() { n.statsdLoop() })
	}
}

// nsqd元数据结构体
// 当nsqd进程退出时，将内存中的数据写入到磁盘
// 当nsqd进程启动时，将磁盘中的数据读入到内存
type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"` // topic状态
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json:"paused"` // channel状态
		} `json:"channels"`
	} `json:"topics"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

func oldMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, fmt.Sprintf("nsqd.%d.dat", opts.ID))
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

func (n *NSQD) LoadMetadata() error {
	// isLoading是一个flag
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)

	// 返回一个新文件路径，data-path/nsqd.dat
	fn := newMetadataFile(n.getOpts())
	// 也是返回一个文件路径，data-path/nsqd.$ID.data
	// old metadata filename with ID, maintained in parallel to enable roll-back
	fnID := oldMetadataFile(n.getOpts())

	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	dataID, errID := readOrEmpty(fnID)
	if errID != nil {
		return errID
	}

	if data == nil && dataID == nil {
		return nil // fresh start
	}
	if data != nil && dataID != nil {
		if bytes.Compare(data, dataID) != 0 {
			return fmt.Errorf("metadata in %s and %s do not match (delete one)", fn, fnID)
		}
	}

	// 二者取其一，用metadata
	if data == nil {
		// only old metadata file exists, use it
		fn = fnID
		data = dataID
	}

	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	for _, t := range m.Topics {
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		// 没有就创建，并且还要从nsqlookupd去获取这个topic的channel name来创建channel
		topic := n.GetTopic(t.Name)
		if t.Paused {
			topic.Pause()
		}

		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name)
			if c.Paused {
				channel.Pause()
			}
		}
	}
	return nil
}

// 将topic和channel信息写入磁盘
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(n.getOpts())
	// old metadata filename with ID, maintained in parallel to enable roll-back
	// fileNameID文件是fileName文件的软连接，内容和fileName一样，用于回滚操作。文件命名为nsqd.867.dat
	// nsqd.867.dat这里的867是opts.ID，根据主机名的哈希值算出的0-1024之间的值
	fileNameID := oldMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	// 遍历所有的topic和channel。注：在调用PersistMetadata之前就加上了lock锁
	// 拿到所有topic和channel的name和paused字段，paused描述该topic或channel的状态
	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		// 临时topic跳过不存
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if channel.ephemeral {
				channel.Unlock()
				continue
			}
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channels = append(channels, channelData)
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// 先写到临时文件，再rename。这样可以保证别的goroutine或外部进程对此文件的读写，传输是安全的。
	// 而且即使此时程序挂掉，原来的文件还在。
	// redis在进行备份时即如此：RDB会先写到临时文件，完了再Rename，这样外部程序对RDB文件的备份和传输过程是安全的。而且即使写新快照的过程中Server被强制关掉了，旧的RDB文件还在。
	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	stat, err := os.Lstat(fileNameID)
	if err == nil && stat.Mode()&os.ModeSymlink != 0 {
		return nil
	}

	// if no symlink (yet), race condition:
	// crash right here may cause next startup to see metadata conflict and abort

	tmpFileNameID := fmt.Sprintf("%s.%d.tmp", fileNameID, rand.Int())

	if runtime.GOOS != "windows" {
		// 临时文件是一个符号链接（软链接）
		err = os.Symlink(fileName, tmpFileNameID)
	} else {
		// on Windows need Administrator privs to Symlink
		// instead write copy every time
		err = writeSyncFile(tmpFileNameID, data)
	}
	if err != nil {
		return err
	}

	// 最终生成的nsqd.733.dat是一个符号链接，指向nsqd.dat
	err = os.Rename(tmpFileNameID, fileNameID)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

func (n *NSQD) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	close(n.exitChan)
	n.waitGroup.Wait()

	n.dl.Unlock()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first.
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	n.Lock()

	// 这里是一个双重检查加锁
	// 为什么上面不加一个Lock，而是加一个读锁呢？考虑到GetTopic是常规操作，大多数情况下会命中数据而直接返回
	// 用Lock的话有点浪费了。双重检查是防止在加读锁期间有goroutine修改了topicMap。
	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	// 创建了一个Topic，并启动goroutine将Topic上的msg写入到Topic下的所有channel中
	t = NewTopic(topicName, &context{n}, deleteCallback)
	n.topicMap[topicName] = t

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)

	// release our global nsqd lock, and switch to a more granular topic lock while we init our
	// channels from lookupd. This blocks concurrent PutMessages to this topic.
	t.Lock()
	n.Unlock()

	// 从nsqlookupd中取回topic下的channel并创建好，以使发送到topic的msg能被正确投递到channel
	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		// 返回的是去重排序后的channel names
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") {
				// we don't want to pre-create ephemeral channels
				// because there isn't a client connected
				continue
			}
			// 内部未加锁，这里用的还是topic的锁
			t.getOrCreateChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	t.Unlock()

	// NOTE: I would prefer for this to only happen in topic.GetChannel() but we're special
	// casing the code above so that we can control the locks such that it is impossible
	// for a message to be written to a (new) topic while we're looking up channels
	// from lookupd...
	//
	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:
			if !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			// 没有必要进行扩缩容
			break
		} else if idealPoolSize < n.poolSize {
			// contract 收缩
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			// 这个goroutine放在resizePool中来做，总觉得地方不合适
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
// dirty表示确实有超时和延迟发送的消息存在
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// 处理正在投递中和延迟投递的消息
// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)

	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}
