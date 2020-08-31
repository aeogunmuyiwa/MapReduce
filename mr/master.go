package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "sync"

type Master struct {
	// Your definitions here.
	sync.Mutex

	masterAddress string
	registerChannel chan string
	doneChannel chan bool

	newCond sync.Cond
	workers []string // rpc address for each workeer
	jobName string //current running  jon

	files [] string //Input files

	nReduce int // reduce partition

	shutdown chan struct{}
	stats []int
	statTime  time.Time
	MapTime float64
	ElapsedTime float64
	l  net.Listener
}

// Your code here -- RPC handlers for the worker to call.
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// reply.Y = args.X + 1
// return nil
//}
//
// start a thread that listens for RPCs from worker.go
//
/************Register master when works start*************/
func (m *Master)Register(args *RegisterArgs, _ *struct{}) error {
	m.Lock()
	defer m.Unlock()
	fmt.Printf("Register : worker %s\n", args.Worker)

	m.workers = append(m.workers, args.Worker)
	//m.newCond.Broadcast() // new worker entry
	fmt.Printf("Registered : worker %s\n", args.Worker)
	go func() {
		println("here")
		m.registerChannel <- args.Worker
		 m.runMaster( m.schedule, func() {
			m.stats = m.killWorkers()
			m.stopRPCServer()
		})

	}()



	//}()
	return  nil
	//go m.runMaster(m.jobName, m.nReduce,m.files, m.schedule ,func() {
	//		m.stats = m.killWorkers()
	//		m.server()
	//	})

	//go m.runMaster(m.jobName,m.nReduce,m.files,m.schedule, func() {
	//	println("Restarting")
	//	m.stats = m.killWorkers()
	//	m.startRPCServer()
	//})

	//fmt.Printf("%s: startinf map/rask %s\n", m.masterAddress, m.jobName)
	//m.statTime = time.Now()
	//m.schedule(mapPhase)
	//m.MapTime = time.Since(m.statTime).Seconds()
	//fmt.Printf("================starting map reduce")
	//m.schedule(reducePhase)
	////m.finish()
	//m.merge()
	//fmt.Printf("%s: MAP REDUCE TASK COMPLETED \n", m.masterAddress)
	//m.ElapsedTime = time.Since(m.statTime).Seconds()
	//fmt.Printf("Map time is %v s\n", m.MapTime)
	//fmt.Printf("The program finished in %v s\n", m.ElapsedTime)
	//m.doneChannel <- true
	//
	//m.stats = m.killWorkers()
	//m.startRPCServer()


	//return  nil
}
/**********************************/
// helper function that sends information about all existing
// and newly registered workers to channel ch. schedule()
// reads ch to learn about workers.
func (m *Master) forwardRegistrations(ch chan string) {
	var wg sync.WaitGroup
	wg.Add(2)
	locker := sync.Mutex{}
	locker.Lock()

	c := sync.NewCond(&locker)

	i := 0
	for {

		//m.Lock()

		locker.Lock()
		defer  locker.Unlock()

		if len(m.workers) > i {
			// there's a worker that we haven't told schedule() about.
			w := m.workers[i]
			fmt.Println(" ch enqueue: %s", w)
			go func() { ch <- w }() // send without holding the lock.
			i = i + 1
		} else {
			// wait for Register() to add an entry to workers[]
			// in response to an RPC from a new worker.
		//	m.newCond.Wait()
			c.Wait()
		}
		m.Unlock()
		wg.Done()
	}
}


// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
/******Make master ****************/
func MakeNewMaster ()(m *Master){
	m = new(Master)
	m.masterAddress =  masterSock()
	m.shutdown = make(chan struct{})
	m.doneChannel = make(chan  bool)
	m.registerChannel = make(chan string)
	return
}
/**********************************/
func MakeMaster(files []string, nReduce int) *Master {
	m := MakeNewMaster()
	//m.masterAddress =  masterSock()
	println("master address %s\n", m.masterAddress)

//	jobName := strconv.Itoa(int(rand.Intn(30)))


	m.jobName = "jobName"
	m.files = files
	m.nReduce = nReduce
	// Your code here.
	m.startRPCServer()
	ch := make(chan  string)
	go m.forwardRegistrations(ch)
	go m.runMaster( m.schedule, func() {
		m.stats = m.killWorkers()
		m.stopRPCServer()
	})
	return m
	//fmt.Printf("%s: startinf map/rask %s\n", m.masterAddress, m.jobName)
	//m.statTime = time.Now()
	//m.schedule(mapPhase)
	//m.MapTime = time.Since(m.statTime).Seconds()
	//fmt.Printf("================starting map reduce")
	//m.schedule(reducePhase)
	////m.finish()
	//m.merge()
	//fmt.Printf("%s: MAP REDUCE TASK COMPLETED \n", m.masterAddress)
	//m.ElapsedTime = time.Since(m.statTime).Seconds()
	//fmt.Printf("Map time is %v s\n", m.MapTime)
	//fmt.Printf("The program finished in %v s\n", m.ElapsedTime)
	//m.doneChannel <- true
	//
	//m.stats = m.killWorkers()
	//m.startRPCServer()

	//return m
}



func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// calls (Register in particular) for as long as the worker is alive.
func (m *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(m)
	os.Remove(m.masterAddress) // only needed for "unix"
	fmt.Println("starting rpc server with master address :",m.masterAddress)
	l, e := net.Listen("unix", m.masterAddress)
	if e != nil {
		log.Fatal("RegstrationServer", m.masterAddress, " error: ", e)
	}else{
		fmt.Println("start rpcserver ")
	}
	m.l = l
	fmt.Println(l)



	// now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	go func() {
		fmt.Println(",master in ")
	loop:
		for {
			select {
			case <-m.shutdown:
				fmt.Println("master shudown while waiting ")
				break loop
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				println("Accepitng worker connetion")
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				fmt.Println("RegistrationServer: accept error", err)
				break
			}
		}
		fmt.Println("RegistrationServer: done\n")
	}()
	fmt.Println("DONE SERVER")
}
//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	//// Your code here.
	//select {
	//case m.doneChannel <- true:
	//	ret = true
	//default:
	//	ret = false
	//}
	return ret
}
//
func (m *Master) Wait() {
	<-m.doneChannel
}


/**********************************/


/*************determine if a task is a map or reduce task**************/
type jobPhase string
const (
	mapPhase jobPhase = "Map"
	reducePhase jobPhase = "Reduce"
)
/**********************************/
/***************Execute mapredue on given number of worker ********/
func (m *Master) runMaster(schedule func(phase jobPhase), finish func() ) {

	fmt.Printf("%s: startinf map/rask %s\n", m.masterAddress, m.jobName)
	m.statTime = time.Now()
	schedule(mapPhase)
	m.MapTime = time.Since(m.statTime).Seconds()
	fmt.Printf("================starting map reduce")
	schedule(reducePhase)
	finish()
	m.merge()
	fmt.Printf("%s: MAP REDUCE TASK COMPLETED \n", m.masterAddress)
	m.ElapsedTime = time.Since(m.statTime).Seconds()
	fmt.Printf("Map time is %v s\n", m.MapTime)
	fmt.Printf("The program finished in %v s\n", m.ElapsedTime)
	m.doneChannel <- true
}
/**********************************/
/**********************************/
/**reference https://github.com/dtccx/Mapreduce/blob/cc5766/src/mapreduce/mapreduce.go **** for the merge file code/
/************merge files**********************/
func (m *Master) merge(){
	fmt.Printf("Merging")
	kvs := make(map[string]string)
	for i := 0; i < m.nReduce; i++ {
		p := mergeName(m.jobName, i)
		fmt.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	file, err := os.Create("mr-tmp." + m.jobName)
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}
// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mr." + jobName + "-out-" + strconv.Itoa(reduceTask)
}
// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mr." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}
// removeFile is a simple wrapper around os.Remove that logs errors.
func removeFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}
// CleanupFiles removes all intermediate files produced by running mapreduce.
func (m *Master) CleanupFiles() {
	for i := range m.files {
		for j := 0; j < m.nReduce; j++ {
			removeFile(reduceName(m.jobName, i, j))
		}
	}
	for i := 0; i < m.nReduce; i++ {
		removeFile(mergeName(m.jobName, i))
	}
	removeFile("mr." + m.jobName)
}
/*************send workers shutdown rpc****************/
// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has performed.
func (m *Master) killWorkers() []int {
	m.Lock()
	defer m.Unlock()
	ntasks := make([]int, 0, len(m.workers))
	for _, w := range m.workers {
		fmt.Println("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := ServiceCall( w,"Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			ntasks = append(ntasks, reply.Ntasks)
		}
	}
	return ntasks
}
/*******************/
// server thread and the current thread.
func (m *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call("Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", m.masterAddress)
	}
	fmt.Println("cleanupRegistration: done\n")
}
// Shutdown is an RPC method that shuts down the Master's RPC server.
func (m *Master) Shutdown(_, _ *struct{}) error {
	fmt.Println("Shutdown: registration server\n")
	close(m.shutdown)
	m.l.Close() // causes the Accept to fail
	return nil
}

func call( rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	c, err := rpc.DialHTTP("unix", masterSock())
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}