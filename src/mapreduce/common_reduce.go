package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// TODO:Your code here (Part I).
	//
	KeyValues := make(map[string][]string, 0)
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTask)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("open file err:", err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := KeyValues[kv.Key]
			if !ok {
				KeyValues[kv.Key] = make([]string, 0)
			}
			KeyValues[kv.Key] = append(KeyValues[kv.Key], kv.Value)
		}
	}
	var keys []string
	for key, _ := range KeyValues {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	resFileName := mergeName(jobName, reduceTask)
	resFile, err := os.Create(resFileName)
	if err != nil {
		fmt.Println("create resfile error:", err)
	}
	defer resFile.Close()

	enc := json.NewEncoder(resFile)
	for _, key := range keys {
		res := reduceF(key, KeyValues[key])
		err := enc.Encode(&KeyValue{key, res})
		if err != nil {
			fmt.Println("encode res error:", err)
		}
	}
}
