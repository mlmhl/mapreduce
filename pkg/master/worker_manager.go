package master

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/mlmhl/mapreduce/pkg/rpc"
	"github.com/mlmhl/mapreduce/pkg/types"

	"github.com/golang/glog"
)

const (
	defaultWorkerTimeout        = time.Minute
	defaultHealthyCheckInterval = time.Second * 30
)

type worker struct {
	name       string
	lastActive time.Time

	client *rpc.Client
}

func (w *worker) Address() rpc.Address {
	return w.client.Address()
}

func (w *worker) Execute(task *types.Task, result *types.Result) error {
	return w.client.Call(fmt.Sprintf("%s.Execute", w.name), task, result)
}

func (w *worker) Shutdown() error {
	defer w.client.Close()
	return w.client.Call(fmt.Sprintf("%s.Shutdown", w.name), rpc.NopValue{}, &rpc.NopValue{})
}

func (w *worker) String() string {
	address := w.client.Address()
	return fmt.Sprintf("%s/%s/%s", w.name, address.Network, address.Address)
}

func newWorkerQueue() workerQueue {
	return workerQueue{
		workerIndices:  make(map[string]int),
		workerBalances: make(map[string]int),
	}
}

// Priority queue of workers.
type workerQueue struct {
	workers        []worker
	workerIndices  map[string]int
	workerBalances map[string]int
}

func (wq *workerQueue) Len() int {
	return len(wq.workers)
}

func (wq *workerQueue) Less(i, j int) bool {
	return wq.balance(i) > wq.balance(j)
}

func (wq *workerQueue) Swap(i, j int) {
	wq.workers[i], wq.workers[j] = wq.workers[j], wq.workers[i]
	wq.workerIndices[wq.workers[i].name], wq.workerIndices[wq.workers[j].name] = i, j
}

func (wq *workerQueue) Push(x interface{}) {
	w, ok := x.(worker)
	if !ok {
		return
	}
	wq.workerIndices[w.name] = len(wq.workers)
	wq.workers = append(wq.workers, w)
}

func (wq *workerQueue) Pop() interface{} {
	index := len(wq.workers) - 1
	w := wq.workers[index]
	wq.workers = wq.workers[:index]
	delete(wq.workerIndices, w.name)
	return w
}

func (wq *workerQueue) contains(name string) bool {
	_, exist := wq.workerIndices[name]
	return exist
}

func (wq *workerQueue) list() []worker {
	workers := make([]worker, len(wq.workers))
	copy(workers, wq.workers)
	return workers
}

func (wq *workerQueue) add(w worker, balance int) {
	heap.Push(wq, w)
	wq.workerBalances[w.name] = balance
}

func (wq *workerQueue) remove(name string) {
	heap.Remove(wq, wq.workerIndices[name])
	delete(wq.workerBalances, name)
}

func (wq *workerQueue) allocate() (worker, bool) {
	if len(wq.workers) == 0 || wq.balance(0) <= 0 {
		return worker{}, false
	}
	w := wq.workers[0]
	wq.workerBalances[w.name]--
	heap.Fix(wq, 0)

	return w, true
}

func (wq *workerQueue) release(name string) {
	wq.workerBalances[name]++
	heap.Fix(wq, wq.index(name))
}

func (wq *workerQueue) update(name string, balance int) int {
	index := wq.index(name)
	oldBalance := wq.balance(index)
	wq.workerBalances[name] = balance
	heap.Fix(wq, index)
	return oldBalance
}

func (wq *workerQueue) heartBeat(name string, lastActive time.Time) {
	wq.workers[wq.index(name)].lastActive = lastActive
}

func (wq *workerQueue) clear() {
	wq.workers, wq.workerIndices, wq.workerBalances = nil, nil, nil
}

func (wq *workerQueue) index(name string) int {
	return wq.workerIndices[name]
}

func (wq *workerQueue) balance(index int) int {
	return wq.workerBalances[wq.workers[index].name]
}

type workerManager interface {
	Run()
	Stop()
	Add(worker worker, balance int)
	Taint(name string)
	Allocate() (worker, bool)
	Release(name string)
	HeartBeat(name string)
}

func newWorkerManager(workerTimeout time.Duration, healthyCheckInterval time.Duration) workerManager {
	if workerTimeout == 0 {
		workerTimeout = defaultWorkerTimeout
	}
	if healthyCheckInterval == 0 {
		healthyCheckInterval = defaultHealthyCheckInterval
	}

	lock := &sync.Mutex{}
	return &defaultWorkerManager{
		lock: lock,
		cond: sync.NewCond(lock),

		workerTimeout:        workerTimeout,
		healthyCheckInterval: healthyCheckInterval,

		queue: newWorkerQueue(),
	}
}

type defaultWorkerManager struct {
	cond *sync.Cond
	lock *sync.Mutex

	workerTimeout        time.Duration
	healthyCheckInterval time.Duration

	queue   workerQueue
	stopped bool
}

func (wm *defaultWorkerManager) Run() {
	glog.Infof("Worker manager started")
	go wm.unhealthyWorkerCleanupLoop()
}

func (wm *defaultWorkerManager) Stop() {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	for _, w := range wm.queue.list() {
		if err := w.Shutdown(); err != nil {
			glog.Errorf("Shut down worker failed: %s, %v", w.String(), err)
		}
	}
	wm.queue.clear()
	wm.stopped = true
	wm.cond.Broadcast()
}

func (wm *defaultWorkerManager) Add(worker worker, balance int) {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if wm.stopped {
		glog.Warningf("Skip add worker(%s) as workerManager stopped", worker.String())
		return
	}

	if wm.queue.contains(worker.name) {
		wm.queue.update(worker.name, balance)
	} else {
		wm.queue.add(worker, balance)
		glog.V(3).Infof("Add a new worker: %s", worker.String())
	}
	wm.cond.Broadcast()
}

func (wm *defaultWorkerManager) Taint(name string) {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if wm.stopped {
		glog.Warningf("Skip taint worker(%s) as workerManager stopped", name)
		return
	}

	if !wm.queue.contains(name) {
		glog.Warning("Worker %s not exist, skip Taint operation", name)
	}
	wm.queue.remove(name)
}

func (wm *defaultWorkerManager) Allocate() (worker, bool) {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	for {
		if wm.stopped {
			return worker{}, true
		}
		worker, available := wm.queue.allocate()
		if available {
			return worker, false
		}
		wm.cond.Wait()
	}
}

func (wm *defaultWorkerManager) Release(name string) {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if wm.stopped {
		glog.Warningf("Skip release worker(%s) as workerManager stopped", name)
		return
	}

	if !wm.queue.contains(name) {
		glog.Warningf("Worker %s not exist, skip Release operation", name)
		return
	}

	wm.queue.release(name)
	wm.cond.Broadcast()
}

func (wm *defaultWorkerManager) HeartBeat(name string) {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if wm.stopped {
		glog.Warningf("Skip heartBeat worker(%s) as workerManager stopped", name)
		return
	}

	if !wm.queue.contains(name) {
		glog.Warningf("Worker %s not exist, skip Update operation", name)
		return
	}
	wm.queue.heartBeat(name, time.Now())
}

func (wm *defaultWorkerManager) unhealthyWorkerCleanupLoop() {
	timer := time.NewTimer(wm.healthyCheckInterval)
	for {
		if !wm.cleanupUnhealthyWorkers() {
			glog.V(3).Infof("Stop clean up unhealthy workers")
			break
		}
		<-timer.C
		timer.Reset(wm.healthyCheckInterval)
	}
}

func (wm *defaultWorkerManager) cleanupUnhealthyWorkers() bool {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if wm.stopped {
		return false
	}

	now := time.Now()
	for _, worker := range wm.queue.list() {
		if worker.lastActive.Add(wm.workerTimeout).Before(now) {
			glog.Warningf("Worker %s out of contact for %s, remove it", worker.name, wm.workerTimeout)
			wm.queue.remove(worker.name)
		}
	}

	return true
}
