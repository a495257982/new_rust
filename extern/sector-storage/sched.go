package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[storiface.WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	//added by jack
	workersip     map[WorkerID]string
	fixedLK       sync.RWMutex
	fixedp1worker map[abi.SectorID]WorkerID
	//ENDING

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources // use with workerHandle.lk
	active    *activeResources // use with workerHandle.lk

	lk sync.Mutex // can be taken inside sched.workersLk.RLock

	wndLk         sync.Mutex // can be taken inside sched.workersLk.RLock
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker storiface.WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	activeWindows []*schedWindow
	wid           storiface.WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    float64
	cpuUse     uint64

	cond    *sync.Cond
	waiting int
}

type workerRequest struct {
	sector   storage.SectorRef
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

// added by jack
func get_taskfile() string {
	miner_path, ok := os.LookupEnv("LOTUS_MINER_PATH")
	if ok {
		//log.Info("miner_path:", miner_path)
	} else {
		//log.Warn("none miner_path")
	}
	filename := path.Join(miner_path, "task.json")

	return filename
}

func (sh *scheduler) sync_taskfile() error {
	sh.fixedLK.Lock()
	mp1worker := make(map[string]string)
	for k, v := range sh.fixedp1worker {
		str := storiface.SectorName(k)
		mp1worker[str] = uuid.UUID(v).String()
	}
	sh.fixedLK.Unlock()
	data, err := json.Marshal(mp1worker)
	if err != nil {
		log.Error(string(data))
		return err
	}
	err = ioutil.WriteFile(get_taskfile(), data, 0644)
	if err != nil {
		log.Error("sync task list to file failed!", err)
		return err
	}

	return nil
}

func (sh *scheduler) load_taskfile() error {
	mp1worker := make(map[string]string)
	data, err := ioutil.ReadFile(get_taskfile())
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &mp1worker)
	if err != nil {
		return err
	}

	for k, v := range mp1worker {
		var n abi.SectorNumber
		var mid abi.ActorID
		read, err := fmt.Sscanf(k, "s-t0%d-%d", &mid, &n)
		if err != nil || read != 2 {
			log.Error("load_taskfile errored 1!", err)
			return err
		}
		uid, err := uuid.Parse(v)
		if err != nil {
			log.Error("load_taskfile errored 2!", err)
			return err
		}
		sh.fixedLK.Lock()
		sh.fixedp1worker[abi.SectorID{Miner: mid, Number: n}] = WorkerID(uid)
		sh.fixedLK.Unlock()
	}
	log.Infof("load fixed task completed!, task map table is: %+v", sh.fixedp1worker)

	return nil
}

//ENDING

func newScheduler() *scheduler {
	//added by jack
	/*return &scheduler{
	workers: map[WorkerID]*workerHandle{},*/
	sh := &scheduler{
		workers:       map[storiface.WorkerID]*workerHandle{}, //added by pan
		workersip:     map[WorkerID]string{},
		fixedp1worker: make(map[abi.SectorID]WorkerID),
		//ENDING

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:     map[storiface.CallID]struct{}{},
			running:  map[storiface.CallID]trackedWork{},
			prepared: map[uuid.UUID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	//added by jack
	sh.load_taskfile()
	return sh
	//ENDING
}

func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait)
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			sh.schedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.openWindows = append(sh.openWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.schedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.todo {
						sh.schedQueue.Push(request)
					}
				}

				openWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
				for _, window := range sh.openWindows {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindows = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySched() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	windowsLen := len(sh.openWindows)
	queueLen := sh.schedQueue.Len()

	log.Debugf("SCHED %d queued; %d open windows", queueLen, windowsLen)

	if windowsLen == 0 || queueLen == 0 {
		// nothing to schedule on
		return
	}

	windows := make([]schedWindow, windowsLen)
	acceptableWindows := make([][]int, queueLen)

	// Step 1
	throttle := make(chan struct{}, windowsLen)

	var wg sync.WaitGroup
	wg.Add(queueLen)
	for i := 0; i < queueLen; i++ {
		throttle <- struct{}{}

		go func(sqi int) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]

			task.indexHeap = sqi
			for wnd, windowRequest := range sh.openWindows {
				// added by jack
				skip := false
				fixed := func(wid WorkerID) {
					for wnd1, wiwindowRequest1 := range sh.openWindows {
						if wiwindowRequest1.worker == storiface.WorkerID(wid) {
							wnd = wnd1
							windowRequest = wiwindowRequest1
							skip = true
							break
						}
					}
				}
				switch task.taskType {
				case sealtasks.TTAddPiece:
					if homedir, err := homedir.Expand("~"); err == nil {
						_, err := os.Stat(filepath.Join(homedir, "./FixedSectorWorkerId"))
						notexist := os.IsNotExist(err)
						if !notexist {
							data, err := os.ReadFile(path.Join(homedir, "./FixedSectorWorkerId", storiface.SectorName(task.sector.ID)+".cfg"))
							if err == nil {
								if len(string(data)) == 36 {
									sh.fixedLK.Lock()
									sh.fixedp1worker[task.sector.ID] = WorkerID(uuid.MustParse(string(data)))
									sh.fixedLK.Unlock()
									sh.sync_taskfile()
									fixed(WorkerID(uuid.MustParse(string(data))))
								}
							}
						}
					}
				case sealtasks.TTPreCommit1:
					sh.fixedLK.Lock()
					wid, ok := sh.fixedp1worker[task.sector.ID]
					sh.fixedLK.Unlock()
					if ok {
						fixed(wid)
					}
				case sealtasks.TTPreCommit2:
					sh.fixedLK.Lock()
					wid, ok := sh.fixedp1worker[task.sector.ID]
					if ok {
						ip, ok := sh.workersip[wid]
						if ok && ip != "" {
							for Wid, iip := range sh.workersip {
								if iip == ip && Wid != wid {
									wid = Wid
									break
								}
							}
						}
						fixed(wid)
					}
					sh.fixedLK.Unlock()
				default:
				}
				// TTFinalize任务, 删除task与worker的映射关系
				if task.taskType == sealtasks.TTFinalize {
					sh.fixedLK.Lock()
					_, ok := sh.fixedp1worker[task.sector.ID]
					sh.fixedLK.Unlock()
					if ok {
						sh.fixedLK.Lock()
						delete(sh.fixedp1worker, task.sector.ID)
						sh.fixedLK.Unlock()
						sh.sync_taskfile()
					}
				}
				//ENDING
				worker, ok := sh.workers[windowRequest.worker]
				if !ok {
					log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}

				if !worker.enabled {
					log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
					continue
				}

				needRes := worker.info.Resources.ResourceSpec(task.sector.ProofType, task.taskType)

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info) {
					continue
				}
				//added by jack
				if task.taskType != sealtasks.TTAddPiece && task.taskType != sealtasks.TTPreCommit1 && task.taskType != sealtasks.TTPreCommit2 {
					if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info) {
						continue
					}
				}
				// ENDING

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, task.sector.ProofType, worker)
				cancel()
				if err != nil {
					log.Errorf("trySched(1) req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {
					continue
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
				// added by jack
				if skip == true {
					break
				}
				//ENDING
			}

			if len(acceptableWindows[sqi]) == 0 {
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good)
			//added by jack
			if task.taskType != sealtasks.TTAddPiece && task.taskType != sealtasks.TTPreCommit1 && task.taskType != sealtasks.TTPreCommit2 {
				//added by jack
				rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
					acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
				})
				sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool {
					wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
					wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

					if wii == wji {
						// for the same worker prefer older windows
						return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
					}

					wi := sh.workers[wii]
					wj := sh.workers[wji]

					rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
					defer cancel()

					r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj)
					if err != nil {
						log.Errorf("selecting best worker: %s", err)
					}
					return r
				})
			}
		}(i)
	}

	wg.Wait()

	log.Debugf("SCHED windows: %+v", windows)
	log.Debugf("SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0
	rmQueue := make([]int, 0, queueLen)

	for sqi := 0; sqi < queueLen; sqi++ {
		task := (*sh.schedQueue)[sqi]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.indexHeap] {
			wid := sh.openWindows[wnd].worker
			info := sh.workers[wid].info

			log.Debugf("SCHED try assign sqi:%d sector %d to window %d", sqi, task.sector.ID.Number, wnd)

			needRes := info.Resources.ResourceSpec(task.sector.ProofType, task.taskType)

			// TODO: allow bigger windows
			if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", info) {
				continue
			}

			log.Debugf("SCHED ASSIGNED sqi:%d sector %d task %s to window %d", sqi, task.sector.ID.Number, task.taskType, wnd)

			windows[wnd].allocated.add(info.Resources, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)

		rmQueue = append(rmQueue, sqi)
		scheduled++
	}

	if len(rmQueue) > 0 {
		for i := len(rmQueue) - 1; i >= 0; i-- {
			sh.schedQueue.Remove(rmQueue[i])
		}
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window := window // copy
		select {
		case sh.openWindows[wnd].done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, windowsLen-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
