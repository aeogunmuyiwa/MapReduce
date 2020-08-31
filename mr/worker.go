package mr
import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
/********************/
/*****worker struct ********/
type  WorkerStuct struct {
	sync.Mutex
	name string
	Map func(string, string) []KeyValue
	Reduce func (string, []string) string
	nRPC int
	nTask int
	concurrent int // number of parallel DoTasks in this worker; mutex
	l net.Listener
}
/*******called by master when new task is being put on queue for schedule**/
//func (Worker  *Worker_) DoTask(args *DoTaskArgs, _*struct{}) error {
// fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n", Worker.name,args.Phase, args.TaskNumber,args.File, args.NumberOtherPhase )
// switch args.Phase {
// case mapPhase:
//
// }
//}
//
// main/mrworker.go calls this function.
//
func (Worker *WorkerStuct) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		Worker.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	Worker.Lock()
	Worker.nTask += 1
	Worker.concurrent += 1
	nc := Worker.concurrent
	Worker.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a
		// time to a given worker.
		log.Fatal("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}
	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, Worker.Map)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNumber, mergeName(arg.JobName, arg.TaskNumber),arg.NumOtherPhase,Worker.Reduce)
	}
	Worker.Lock()
	Worker.concurrent -= 1
	Worker.Unlock()

	fmt.Printf("%s: %v task #%d done\n", Worker.name, arg.Phase, arg.TaskNumber)
	return nil
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	fmt.Println("wait for woker task to be scheduled")

	worker := new(WorkerStuct)
	sockname := port("worker"+strconv.Itoa(0))
	worker.name = sockname
	//worker.name = workerName
	worker.Map = mapf
	worker.Reduce = reducef
	worker.nRPC = 100

	rpcs := rpc.NewServer()
	rpcs.Register(worker)

	fmt.Println("Done registering master")


	os.Remove(worker.name) // only needed for "unix"
	l, e := net.Listen("unix", worker.name)
	if e != nil {
		log.Fatal("RunWorker: worker ", worker.name, " error: ", e)
	}
	worker.l = l
	fmt.Println(l)
	worker.register(masterSock())

	//// DON'T MODIFY CODE BELOW
	for {
		fmt.Println("entering worker resiver for loop")
		fmt.Println("WROKER NAME ", worker.name)
		worker.Lock()
		if worker.nRPC == 0 {
			worker.Unlock()
			break
		}
		worker.Unlock()
		conn, err := worker.l.Accept()
		if err == nil {
			worker.Lock()
			worker.nRPC--
			worker.Unlock()
			go func() {
				go rpcs.ServeConn(conn)
				conn.Close()
			}()
			worker.Lock()
			worker.nTask++
			worker.Unlock()


		} else {
			break
		}
	}
	worker.l.Close()
	fmt.Println("RunWorker %s exit\n", masterSock())
}
/*******rpc call to master**/
func (Worker *WorkerStuct) register (master string){
	fmt.Println("registering master")
	args := new(RegisterArgs)

	args.Worker = Worker.name
	fmt.Println("worker name : -  %s\n", Worker.name)
	fmt.Println("master address  is ", master)
	ok := call("Master.Register", args, new(struct{}))
	if ok == false{
		fmt.Printf("Register: RPC %s register error\n")
	}
}
/*************************************/
/********called by master when all task are complete**********/
func (Worker *WorkerStuct) Shutdown (_*struct{}, res *ShutdownReply) error {
	fmt.Printf("Shudtdown %s\n", res.Ntasks)
	Worker.Lock()
	defer Worker.Unlock()
	res.Ntasks = Worker.nTask
	Worker.nRPC = 1
	return nil
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// DoTask is called by the master when a new task is being scheduled on this
// worker.
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
//func  call(sockname string, rpcname string, args interface{}, reply interface{}) bool {
//	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
//
//	c, err := rpc.DialHTTP("unix", sockname)
//	if err != nil {
//		log.Fatal("dialing:", err)
//	}
//	defer c.Close()
//	err = c.Call(rpcname, args, reply)
//	if err == nil {
//		return true
//	}
//	fmt.Println(err)
//	return false
//}
/*************************************/
// reads input file  and partitions into nreduce files
//intermidate is stored in ma
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	//

	fmt.Printf("doMap: %d-%d", mapTaskNumber, nReduce)
	readData, readDataErr := ioutil.ReadFile(inFile)
	if readDataErr != nil {
		return
	}
	content := string(readData)
	kvList := mapF(inFile, content)
	// open all the reduce tmp file
	// and keep all the file handler
	//writeFileList := make([]*File, 0)
	encList := make([]*json.Encoder, 0)
	for i := 0; i < nReduce; i++ {
		filename := reduceName(jobName, mapTaskNumber, i)
		f, createFileErr := os.Create(filename)
		if createFileErr != nil {
			fmt.Println("err happened when create tmp file")
			return
		}
		//writeFileList = append(writeFileList, f)
		enc := json.NewEncoder(f)
		encList = append(encList, enc)
		defer f.Close()
		// keep create file open
	}
	for _, kv := range kvList {
		key := kv.Key
		//fmt.Printf("%s\n", key)
		//value := kv.Value
		reduceTaskNumber := ihash(key) % nReduce
		//writefileName := mergeName(jobName, mapTaskNumber, reduceTaskNumber)
		writeErr := encList[reduceTaskNumber].Encode(&kv)
		if writeErr != nil {
			return
		}
	}
}
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	fmt.Println("doReduce1")
	kvList := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		readFileName := reduceName(jobName, i, reduceTaskNumber)
		readFile, readFileErr := os.Open(readFileName)
		if readFileErr != nil {
			return
		}
		dec := json.NewDecoder(readFile)
		for dec.More() {
			var kv KeyValue
			decErr := dec.Decode(&kv)
			if decErr != nil {
				return
			}
			kvList = append(kvList, kv)
		}
	}
	//close(kvChan)
	//fmt.Println("doReduce chan finish")
	// sorted or not
	// we can skip sort procedure daze!
	kvsMap := make(map[string][]string)
	for _, kv := range kvList {
		if _, ok := kvsMap[kv.Key]; ok {
			// found key in the kvList
			kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
		} else {
			kvsMap[kv.Key] = make([]string, 1)
			kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
		}
	}
	writeFile, writeFileErr := os.Create(outFile)
	if writeFileErr != nil {
		return
	}
	defer writeFile.Close()
	outEnc := json.NewEncoder(writeFile)
	for key, vlist := range kvsMap {
		outEnc.Encode(KeyValue{key, reduceF(key, vlist)})
	}
}